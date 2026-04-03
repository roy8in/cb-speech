"""
Central Bank Watchtower — Hawkish/Dovish NLP Analyzer

Uses a local LLM via Ollama (e.g., llama3) to analyze speeches and assign
a stance score from -1.0 (Dovish) to 1.0 (Hawkish).
"""

import os
import json
import logging
import concurrent.futures
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Import config correctly
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))
from core.config import config

# Ensure environment variables are loaded
load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-2.5-flash"

class KeywordItem(BaseModel):
    category: str = Field(description="General Theme (e.g., Inflation, Labor Market)")
    detail: str = Field(description="Specific Item (e.g., Services prices, Wage growth)")

class StanceResult(BaseModel):
    stance_score: Optional[float] = Field(description="float from the rubric, or null if no policy signal")
    stance_reason: str = Field(description="2-3 sentence explanation citing specific phrases")
    keywords: List[KeywordItem] = Field(description="List of key economic concepts")
    main_risk: Optional[str] = Field(description="The single most significant threat to monetary policy goals mentioned")

SYSTEM_PROMPT = """You are a monetary policy analyst specializing in central bank communications.

TASK: Read the speech and determine the speaker's monetary policy stance, key economic concepts, and the primary risk identified.

SCORING rubric:
 -1.0: Explicitly calls for immediate rate cuts or emergency easing
 -0.7: Strongly emphasizes downside risks, recession fears, need for accommodation
 -0.5: Leans dovish — highlights slowing growth, labor market weakness, or subdued inflation
 -0.3: Mildly dovish — acknowledges risks but suggests patience before tightening
  0.0: Neutral — balanced assessment of risks with no clear directional bias
  null: No monetary policy signal (regulation, financial stability, payments, CBDC, history, etc.)
  0.3: Mildly hawkish — notes inflation persistence, suggests vigilance
  0.5: Leans hawkish — warns of inflation risks, hints at tightening or holding rates high
  0.7: Strongly hawkish — advocates for rate hikes, emphasizes inflation fighting
  1.0: Explicitly calls for immediate rate hikes or aggressive tightening

INSTRUCTIONS:
1. Identify key phrases that reveal the speaker's policy stance and map them to the rubric.
2. Provide a 2-3 sentence 'stance_reason' citing specific evidence for the score.
3. Extract up to 15 key economic concepts. Each concept must be mapped strictly to one of the following 12 categories: [Inflation, Inflation Expectations, Labor Market, Economic Growth, Supply Side/Productivity, Financial Stability, Housing Market, Monetary Policy, Global Economy, Fiscal Policy, Energy & Commodities, Other].
4. Use the 'Other' category only if the concept cannot be logically placed within the first 11 categories.
5. Structure keywords as: {"category": "Category Name", "detail": "Specific indicator or metric (e.g., 'Core PCE', 'Wage growth', 'Demographics')"}.
6. Identify the 'Main Risk': The single most significant threat to achieving policy goals discussed.
7. If no monetary policy signals exist, set stance_score to null.

OUTPUT: A JSON object with exactly four keys: "stance_score", "stance_reason", "keywords", "main_risk".
"""

class HawkDoveAnalyzer:
    def __init__(self, db, model: str = DEFAULT_MODEL):
        self.db = db
        self.model = model
        self.client = None
        self._init_llm()

    def _init_llm(self):
        """GenAI 모델 초기화"""
        api_key = config.SPEECH_API_KEY
        
        if not api_key:
            logger.error("SPEECH_API_KEY is not set in config.")
        else:
            self.client = genai.Client(api_key=api_key)

    def check_api_status(self) -> bool:
        """Check if Gemini client is initialized (API key exists)."""
        if not self.client:
            logger.error("Skipping analysis: API client not initialized.")
            return False
        return True

    def analyze_text(self, text: str, date: str = "", speaker: str = "") -> Optional[Dict[str, Any]]:
        """Sends text to Gemini API and expects a JSON response structured via Pydantic schema."""
        if not self.client:
            return None
            
        # Truncate text. Gemini Flash context window is 1M tokens, so 100k chars is very safe.
        max_chars = 100000 
        truncated_text = text[:max_chars] if text else ""
        if len(text) > max_chars:
            truncated_text += "... [TEXT TRUNCATED]"
            
        user_content = f"Date: {date}\nSpeaker: {speaker}\n\nSpeech Text:\n{truncated_text}\n"
        
        try:
            # Generate content using structured JSON schema
            response = self.client.models.generate_content(
                model=self.model,
                contents=user_content,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=StanceResult,
                    temperature=0.0,
                ),
            )
            
            data = json.loads(response.text)
            
            if "stance_score" in data and "stance_reason" in data:
                if data["stance_score"] is not None:
                    score = float(data["stance_score"])
                    score = max(-1.0, min(1.0, score))
                    score = round(score, 1)
                    data["stance_score"] = score
                
                # keywords and main_risk are now mandatory in schema but good to verify
                return data
            else:
                logger.error(f"Missing expected keys in JSON response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return None

    def _analyze_and_update(self, speech_id: int, title: str, text: str, date: str, speaker: str) -> bool:
        """Analyzes a single speech and updates the database using a fresh connection."""
        import time
        logger.info(f"Analyzing speech [{speech_id}]: {title[:50]}...")
        result = self.analyze_text(text, date=date, speaker=speaker)

        conn = self.db._get_conn()
        try:
            if result:
                status = 'scored' if result.get('stance_score') is not None else 'no_signal'
                conn.execute("""
                    UPDATE speeches 
                    SET stance_score = ?, stance_reason = ?, keywords = ?, main_risk = ?, 
                        analysis_attempts = analysis_attempts + 1,
                        analysis_status = ?
                    WHERE id = ?
                """, (result['stance_score'], result['stance_reason'], json.dumps(result['keywords']), 
                      result['main_risk'], status, speech_id))
                conn.commit()
                logger.info(f"  -> [{speech_id}] Status: {status}, Score: {result['stance_score']}")
            else:
                conn.execute("UPDATE speeches SET analysis_attempts = analysis_attempts + 1 WHERE id = ?", (speech_id,))
                conn.commit()
                logger.warning(f"  -> [{speech_id}] Analysis failed. Attempt logged.")
            
            # Add delay to avoid hitting Google API free tier rate limits (RPM)
            time.sleep(2)
            
            return bool(result)
        finally:
            conn.close()

    def mark_short_speeches_as_skipped(self) -> int:
        """Marks speeches with very short or missing text as 'skipped'."""
        conn = self.db._get_conn()
        try:
            # Mark speeches that are too short to analyze meaningfully or missing text
            cursor = conn.execute("""
                UPDATE speeches 
                SET analysis_attempts = 1, 
                    analysis_status = 'skipped',
                    stance_reason = CASE 
                        WHEN full_text IS NULL THEN 'Skipped: Missing full text.'
                        ELSE 'Skipped: Text too short for meaningful analysis (<= 500 chars).'
                    END,
                    keywords = '[]'
                WHERE (full_text IS NULL OR length(full_text) <= 500)
                AND analysis_status = 'pending'
            """)
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Marked {count} speeches as 'skipped' (short or missing text).")
            return count
        finally:
            conn.close()

    def analyze_pending(self, limit: int = 50, max_workers: int = 2) -> int:
        """Analyzes un-scored speeches up to the given limit in parallel."""
        # First, mark short speeches so they don't clutter the pending queue
        self.mark_short_speeches_as_skipped()
        
        if not self.check_api_status():
            return 0

        conn = self.db._get_conn()
        try:
            # Fetch speeches that are still in 'pending' status
            rows = conn.execute(f"""
                SELECT s.id, s.title, s.full_text, s.date, m.name as speaker
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                WHERE s.full_text IS NOT NULL 
                AND length(s.full_text) > 500
                AND s.analysis_status = 'pending'
                AND s.analysis_attempts < 3
                LIMIT {limit}
            """).fetchall()
        finally:
            conn.close()

        if not rows:
            return 0

        total_to_process = len(rows)
        logger.info(f"Starting parallel analysis for {total_to_process} speeches (max_workers={max_workers})...")

        analyzed_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_speech = {
                executor.submit(
                    self._analyze_and_update,
                    row['id'], row['title'], row['full_text'], row['date'], row['speaker'] or "Unknown"
                ): row['id'] for row in rows
            }

            for future in concurrent.futures.as_completed(future_to_speech):
                try:
                    if future.result():
                        analyzed_count += 1
                except Exception as e:
                    logger.error(f"Worker thread failed: {e}")

        logger.info(f"Parallel analysis complete. Successfully analyzed {analyzed_count}/{total_to_process} speeches.")
        return analyzed_count

s={max_workers})...")

        analyzed_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_speech = {
                executor.submit(
                    self._analyze_and_update,
                    row['id'], row['title'], row['full_text'], row['date'], row['speaker'] or "Unknown"
                ): row['id'] for row in rows
            }

            for future in concurrent.futures.as_completed(future_to_speech):
                try:
                    if future.result():
                        analyzed_count += 1
                except Exception as e:
                    logger.error(f"Worker thread failed: {e}")

        logger.info(f"Parallel analysis complete. Successfully analyzed {analyzed_count}/{total_to_process} speeches.")
        return analyzed_count

