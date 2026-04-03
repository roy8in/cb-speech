import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.scrapers.boc import BOCScraper
from tools.speech_tracker.scrapers.ecb import ECBScraper

def backfill():
    db = SpeechDB()
    boc = BOCScraper(db)
    ecb = ECBScraper(db)
    
    conn = db._get_conn()
    # Find speeches with missing or very short text for BOC and ECB
    rows = conn.execute("""
        SELECT id, bank_code, url, title 
        FROM speeches 
        WHERE (full_text IS NULL OR length(full_text) < 500)
        AND bank_code IN ('BOC', 'ECB')
    """).fetchall()
    conn.close()
    
    print(f"Found {len(rows)} speeches to backfill.")
    
    count = 0
    for row in rows:
        sid, bank, url, title = row['id'], row['bank_code'], row['url'], row['title']
        print(f"[{count+1}/{len(rows)}] Backfilling {bank}: {title[:50]}...")
        
        text = None
        if bank == 'BOC':
            text = boc.fetch_speech_text(url)
        elif bank == 'ECB':
            text = ecb.fetch_speech_text(url)
            
        if text and len(text) > 500:
            db.update_speech_content(sid, text)
            print(f"  -> SUCCESS: {len(text)} chars")
            count += 1
        else:
            print(f"  -> FAILED: Text too short or None")
            
    print(f"\nDone. Successfully backfilled {count} speeches.")

if __name__ == "__main__":
    backfill()
