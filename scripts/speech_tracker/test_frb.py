import time
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.analyzer import HawkDoveAnalyzer

def run_frb_probe():
    db = SpeechDB()
    analyzer = HawkDoveAnalyzer(db)
    conn = db._get_conn()
    
    # Get all unanalyzed speeches by Michael S. Barr
    rows = conn.execute("""
        SELECT s.id, s.title, s.date, m.name, s.full_text 
        FROM speeches s
        JOIN members m ON s.speaker_id = m.id
        LEFT JOIN analysis_results ar ON s.id = ar.speech_id
        WHERE m.name = 'Michael S. Barr'
        AND s.full_text IS NOT NULL 
        AND length(s.full_text) > 1000
        AND ar.stance_score IS NULL
        ORDER BY s.date DESC 
    """).fetchall()
    
    if not rows:
        print("No unanalyzed speeches found for Michael S. Barr.")
        
        # Calculate and print current average
        member = conn.execute("SELECT id, avg_stance_score FROM members WHERE name = 'Michael S. Barr'").fetchone()
        print(f"Current Avg Stance Score: {member['avg_stance_score']}")
        return
        
    print(f"Found {len(rows)} speeches by Michael S. Barr to analyze.\n")
    
    for i, row in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] Analyzing: '{row['title']}' ({row['date']}) - {len(row['full_text'])} chars")
        
        start = time.time()
        result = analyzer.analyze_text(row['full_text'])
        elapsed = time.time() - start
        
        if result:
            conn.execute("""
                INSERT INTO analysis_results
                (speech_id, stance_score, stance_reason, keywords, main_risk, analysis_attempts, analysis_status, analyzed_at, synced_at)
                VALUES (?, ?, ?, ?, ?, 1, 'scored', datetime('now'), NULL)
                ON CONFLICT(speech_id) DO UPDATE SET
                    stance_score = excluded.stance_score,
                    stance_reason = excluded.stance_reason,
                    keywords = excluded.keywords,
                    main_risk = excluded.main_risk,
                    analysis_attempts = analysis_results.analysis_attempts + 1,
                    analysis_status = 'scored',
                    analyzed_at = datetime('now'),
                    synced_at = NULL
            """, (
                row['id'],
                result['stance_score'],
                result['stance_reason'],
                json.dumps(result.get('keywords') or []),
                result.get('main_risk'),
            ))
            conn.commit()
            print(f"  -> Score: {result['stance_score']} | Time: {elapsed:.2f}s")
            print(f"  -> Reason: {result['stance_reason']}\n")
        else:
            print(f"  -> Failed to analyze in {elapsed:.2f}s\n")
            
    # Update the member's moving average
    db.update_all_member_stances()
    
    # Print the new average
    member = conn.execute("SELECT avg_stance_score FROM members WHERE name = 'Michael S. Barr'").fetchone()
    print(f"\nFinal Weighted Avg Stance Score for Michael S. Barr: {member['avg_stance_score']}")

if __name__ == "__main__":
    run_frb_probe()
