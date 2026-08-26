"""Gemini-based hawkish/dovish analysis for central-bank speeches."""

import concurrent.futures
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types
from pydantic import BaseModel, Field


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from core.config import config


logger = logging.getLogger(__name__)
DEFAULT_MODEL = "gemini-2.5-flash"


class KeywordItem(BaseModel):
    category: str = Field(description="General economic theme")
    detail: str = Field(description="Specific indicator or concept")


class StanceResult(BaseModel):
    stance_score: Optional[float] = Field(
        description="Policy stance from -1.0 to 1.0, or null"
    )
    stance_reason: str = Field(description="Evidence for the stance score")
    keywords: List[KeywordItem] = Field(description="Key economic concepts")
    main_risk: Optional[str] = Field(
        description="Primary risk to monetary-policy goals"
    )


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
5. Structure keywords as: {"category": "Category Name", "detail": "Specific indicator or metric"}.
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
        api_key = config.SPEECH_API_KEY
        if not api_key:
            logger.error("SPEECH_API_KEY is not set in config")
            return
        self.client = genai.Client(api_key=api_key)

    def check_api_status(self) -> bool:
        if not self.client:
            logger.error("Skipping analysis: API client not initialized")
            return False
        return True

    def analyze_text(
        self,
        text: str,
        date: str = "",
        speaker: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Analyze one speech and return a structured result."""
        if not self.client:
            return None

        max_chars = 100000
        truncated_text = text[:max_chars] if text else ""
        if text and len(text) > max_chars:
            truncated_text += "... [TEXT TRUNCATED]"

        user_content = (
            f"Date: {date}\n"
            f"Speaker: {speaker}\n\n"
            f"Speech Text:\n{truncated_text}\n"
        )

        try:
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
            if "stance_score" not in data or "stance_reason" not in data:
                logger.error(
                    "Missing expected keys in JSON response: %s",
                    response.text,
                )
                return None

            if data["stance_score"] is not None:
                score = float(data["stance_score"])
                data["stance_score"] = round(
                    max(-1.0, min(1.0, score)),
                    1,
                )
            return data
        except Exception as exc:
            logger.error("LLM analysis failed: %s", exc)
            return None

    def _analyze_and_update(
        self,
        speech_id: int,
        bank_code: str,
        title: str,
        text: str,
        date: str,
        speaker: str,
    ) -> bool:
        logger.info(
            "Analyzing speech [%s] [%s]: %s... (%s)",
            bank_code,
            speech_id,
            title[:50],
            speaker,
        )
        result = self.analyze_text(text, date=date, speaker=speaker)
        conn = self.db._get_conn()

        try:
            if result:
                status = (
                    "scored"
                    if result.get("stance_score") is not None
                    else "no_signal"
                )
                conn.execute(
                    """
                    INSERT INTO analysis_results
                    (
                        speech_id,
                        stance_score,
                        stance_reason,
                        keywords,
                        main_risk,
                        analysis_attempts,
                        analysis_status,
                        analyzed_at
                    )
                    VALUES (?, ?, ?, ?, ?, 1, ?, datetime('now'))
                    ON CONFLICT(speech_id) DO UPDATE SET
                        stance_score = excluded.stance_score,
                        stance_reason = excluded.stance_reason,
                        keywords = excluded.keywords,
                        main_risk = excluded.main_risk,
                        analysis_attempts = analysis_attempts + 1,
                        analysis_status = excluded.analysis_status,
                        analyzed_at = excluded.analyzed_at
                    """,
                    (
                        speech_id,
                        result["stance_score"],
                        result["stance_reason"],
                        json.dumps(result["keywords"]),
                        result["main_risk"],
                        status,
                    ),
                )
                conn.commit()
                logger.info(
                    "  -> [%s] [%s] Status: %s, Score: %s",
                    bank_code,
                    speech_id,
                    status,
                    result["stance_score"],
                )
            else:
                conn.execute(
                    """
                    INSERT INTO analysis_results
                        (speech_id, analysis_attempts, analysis_status)
                    VALUES (?, 1, 'pending')
                    ON CONFLICT(speech_id) DO UPDATE SET
                        analysis_attempts = analysis_attempts + 1
                    """,
                    (speech_id,),
                )
                conn.commit()
                logger.warning(
                    "  -> [%s] [%s] Analysis failed. Attempt logged.",
                    bank_code,
                    speech_id,
                )

            time.sleep(2)
            return bool(result)
        finally:
            conn.close()

    def mark_short_speeches_as_skipped(self) -> int:
        """Mark missing or very short speeches as skipped."""
        conn = self.db._get_conn()
        try:
            cursor = conn.execute(
                """
                INSERT INTO analysis_results
                (
                    speech_id,
                    analysis_attempts,
                    analysis_status,
                    stance_reason,
                    keywords
                )
                SELECT
                    s.id,
                    1,
                    'skipped',
                    CASE
                        WHEN s.full_text IS NULL
                        THEN 'Skipped: Missing full text.'
                        ELSE 'Skipped: Text too short for meaningful analysis (<= 500 chars).'
                    END,
                    '[]'
                FROM speeches s
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE (
                    s.full_text IS NULL
                    OR length(s.full_text) <= 500
                )
                  AND (
                    ar.analysis_status IS NULL
                    OR ar.analysis_status = 'pending'
                  )
                ON CONFLICT(speech_id) DO UPDATE SET
                    analysis_attempts = 1,
                    analysis_status = 'skipped',
                    stance_reason = excluded.stance_reason,
                    keywords = '[]'
                """
            )
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                logger.info(
                    "Marked %s speeches as skipped",
                    count,
                )
            return count
        finally:
            conn.close()

    def revive_skipped_speeches_with_text(self) -> int:
        """Return skipped speeches to pending after full text is recovered."""
        conn = self.db._get_conn()
        try:
            cursor = conn.execute(
                """
                UPDATE analysis_results
                SET analysis_status = 'pending',
                    analysis_attempts = 0,
                    stance_score = NULL,
                    stance_reason = NULL,
                    keywords = NULL,
                    main_risk = NULL,
                    analyzed_at = NULL
                WHERE analysis_status = 'skipped'
                  AND speech_id IN (
                    SELECT id
                    FROM speeches
                    WHERE full_text IS NOT NULL
                      AND length(full_text) > 500
                  )
                """
            )
            conn.commit()
            count = cursor.rowcount
            if count > 0:
                logger.info(
                    "Revived %s skipped speeches with sufficient text",
                    count,
                )
            return count
        finally:
            conn.close()

    def analyze_pending(
        self,
        limit: int = 50,
        max_workers: int = 2,
    ) -> int:
        """Analyze pending speeches in parallel."""
        self.revive_skipped_speeches_with_text()
        self.mark_short_speeches_as_skipped()

        if not self.check_api_status():
            return 0

        conn = self.db._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    s.id,
                    s.bank_code,
                    s.title,
                    s.full_text,
                    s.date,
                    m.name AS speaker
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE s.full_text IS NOT NULL
                  AND length(s.full_text) > 500
                  AND (
                    ar.analysis_status IS NULL
                    OR ar.analysis_status = 'pending'
                  )
                  AND (
                    ar.analysis_attempts IS NULL
                    OR ar.analysis_attempts < 3
                  )
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            return 0

        logger.info(
            "Starting parallel analysis for %s speeches (max_workers=%s)",
            len(rows),
            max_workers,
        )
        analyzed_count = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:
            future_to_speech = {
                executor.submit(
                    self._analyze_and_update,
                    row["id"],
                    row["bank_code"],
                    row["title"],
                    row["full_text"],
                    row["date"],
                    row["speaker"] or "Unknown",
                ): row["id"]
                for row in rows
            }

            for future in concurrent.futures.as_completed(
                future_to_speech
            ):
                try:
                    if future.result():
                        analyzed_count += 1
                except Exception as exc:
                    logger.error("Worker thread failed: %s", exc)

        logger.info(
            "Parallel analysis complete. Successfully analyzed %s/%s speeches.",
            analyzed_count,
            len(rows),
        )
        return analyzed_count
