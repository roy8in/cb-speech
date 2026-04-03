import time
import argparse
import logging
import os
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.analyzer import HawkDoveAnalyzer

# Simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def run_analysis(since_year=None, limit=5000, reanalyze=False):
    db = SpeechDB()
    analyzer = HawkDoveAnalyzer(db)
    
    if not analyzer.check_api_status():
        print("Error: API Key missing.")
        return

    conn = db._get_conn()
    success_count = 0
    print(f"--- Starting Fresh Analysis (Attempts = 0 First) ---")

    try:
        while success_count < limit:
            # 1. Target ONLY speeches that have NEVER been attempted yet
            query = """
                SELECT s.id, s.title, s.date, m.name as speaker, s.full_text 
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE s.full_text IS NOT NULL 
                AND length(s.full_text) > 500
                AND (ar.analysis_attempts IS NULL OR ar.analysis_attempts = 0)
            """
            
            if since_year:
                query += f" AND s.date >= '{since_year}-01-01'"
                
            query += " ORDER BY s.date DESC LIMIT 1"
            
            row = conn.execute(query).fetchone()
            
            # If no more fresh speeches, try items with low attempt counts but no keywords
            if not row:
                print("\n[Switch] No more fresh speeches. Trying failed ones...")
                query = """
                    SELECT s.id, s.title, s.date, m.name as speaker, s.full_text 
                    FROM speeches s
                    LEFT JOIN members m ON s.speaker_id = m.id
                    LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                    WHERE s.full_text IS NOT NULL 
                    AND length(s.full_text) > 500
                    AND (ar.keywords IS NULL OR ar.keywords = '[]')
                    AND ar.analysis_attempts BETWEEN 1 AND 5
                """
                if since_year:
                    query += f" AND s.date >= '{since_year}-01-01'"
                query += " ORDER BY s.date DESC LIMIT 1"
                row = conn.execute(query).fetchone()

            if not row:
                print("\n[Done] All target speeches analyzed.")
                break
                
            speech_id = row['id']
            title = row['title']
            date = row['date']
            text = row['full_text']
            speaker = row['speaker'] or "Unknown"

            print(f"\n[#{success_count+1}] Analyzing ID {speech_id}: '{title[:40]}...' ({date})")
            
            try:
                result = analyzer.analyze_text(text, date=date, speaker=speaker)
                
                if result and isinstance(result, dict):
                    status = 'scored' if result.get('stance_score') is not None else 'no_signal'
                    conn.execute("""
                        INSERT INTO analysis_results 
                        (speech_id, stance_score, stance_reason, keywords, main_risk, analysis_attempts, analysis_status, analyzed_at)
                        VALUES (?, ?, ?, ?, ?, 1, ?, datetime('now'))
                        ON CONFLICT(speech_id) DO UPDATE SET
                            stance_score = excluded.stance_score,
                            stance_reason = excluded.stance_reason,
                            keywords = excluded.keywords,
                            main_risk = excluded.main_risk,
                            analysis_attempts = analysis_attempts + 1,
                            analysis_status = excluded.analysis_status,
                            analyzed_at = excluded.analyzed_at
                    """, (speech_id, result.get('stance_score'), result.get('stance_reason'), 
                          json.dumps(result.get('keywords')), result.get('main_risk'), status))
                    conn.commit()
                    success_count += 1
                    print(f"  -> SUCCESS: Score={result.get('stance_score')}")
                    time.sleep(3) 
                else:
                    # Increment attempts to move it to the back of the queue
                    conn.execute("""
                        INSERT INTO analysis_results (speech_id, analysis_attempts, analysis_status)
                        VALUES (?, 1, 'pending')
                        ON CONFLICT(speech_id) DO UPDATE SET
                            analysis_attempts = analysis_attempts + 1
                    """, (speech_id,))
                    conn.commit()
                    print(f"  -> SKIPPED (Null).")
                    time.sleep(1)

            except Exception as e:
                print(f"  !! Error: {e}")
                conn.execute("""
                    INSERT INTO analysis_results (speech_id, analysis_attempts, analysis_status)
                    VALUES (?, 1, 'pending')
                    ON CONFLICT(speech_id) DO UPDATE SET
                        analysis_attempts = analysis_attempts + 1
                """, (speech_id,))
                conn.commit()
                time.sleep(5)
            
            if success_count > 0 and success_count % 20 == 0:
                db.update_all_member_stances()

    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--since', type=int)
    parser.add_argument('--limit', type=int, default=5000)
    args = parser.parse_args()
    run_analysis(since_year=args.since, limit=args.limit)
