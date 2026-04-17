
import sys
import re
import sqlite3
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from tools.speech_tracker.models import SpeechDB

def fix_boe_dates():
    db = SpeechDB()
    conn = db._get_conn()
    
    # 1. Get all BOE speeches with potential placeholder dates
    query = """
        SELECT id, title, date, url, full_text 
        FROM speeches 
        WHERE bank_code = 'BOE' 
        AND (date LIKE '2021-%-01' OR date LIKE '2022-%-01' OR date LIKE '2023-%-01')
    """
    rows = conn.execute(query).fetchall()
    print(f"Checking {len(rows)} BOE speeches for placeholder dates...")
    
    fixed_count = 0
    
    # Patterns to look for in text
    # 1. 12 March 2021
    # 2. 12 February 2025 (in your examples)
    # 3. Thursday 12 August 2021
    date_patterns = [
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})'
    ]

    for row in rows:
        speech_id = row['id']
        current_date = row['date']
        text = row['full_text']
        url = row['url']
        
        if not text:
            continue
            
        found_date = None
        
        # Search first 2000 characters for a date
        search_text = text[:2000]
        
        for pattern in date_patterns:
            match = re.search(pattern, search_text)
            if match:
                try:
                    if "January" in pattern or "February" in pattern: # First pattern
                        day = int(match.group(1))
                        month_str = match.group(2)
                        year = int(match.group(3))
                        dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
                    else: # Second pattern (Month DD, YYYY)
                        month_str = match.group(1)
                        day = int(match.group(2))
                        year = int(match.group(3))
                        dt = datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
                        
                    found_date = dt.strftime("%Y-%m-%d")
                    
                    # Verify it's in the same month/year as current placeholder
                    if found_date[:7] == current_date[:7]:
                        if found_date != current_date:
                            print(f"Fixing {speech_id}: {current_date} -> {found_date} ({row['title'][:50]})")
                            conn.execute("UPDATE speeches SET date = ?, synced_at = NULL WHERE id = ?", (found_date, speech_id))
                            fixed_count += 1
                        break # Found a valid date
                except Exception as e:
                    continue

    conn.commit()
    conn.close()
    print(f"Finished. Fixed {fixed_count} BOE speech dates.")

if __name__ == "__main__":
    fix_boe_dates()
