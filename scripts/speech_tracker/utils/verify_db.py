import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.models import get_db_path

DB_PATH = get_db_path()

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    errors = 0
    
    # 1. Check if all speeches have a valid speaker_id
    cur.execute("""
        SELECT id, bank_code, speaker_id 
        FROM speeches 
        WHERE speaker_id IS NOT NULL 
          AND speaker_id NOT IN (SELECT id FROM members)
    """)
    orphans = cur.fetchall()
    if orphans:
        print(f"Error: Found {len(orphans)} speeches with invalid speaker_id.")
        errors += 1
    else:
        print("Check 1 passed: All speaker_ids are valid.")
        
    # 2. Check for duplicate speeches (bank_code, date, title, speaker_id)
    cur.execute("""
        SELECT bank_code, date, title, speaker_id, COUNT(*) as cnt
        FROM speeches
        GROUP BY bank_code, date, title, speaker_id
        HAVING cnt > 1
    """)
    duplicates = cur.fetchall()
    if duplicates:
        print(f"Warning: Found {len(duplicates)} speeches with identical bank, date, title, and speaker.")
        for d in duplicates[:5]:
            print(f"  - {d['bank_code']} | {d['date']} | {d['title']} ({d['cnt']} copies)")
        # Note: We do not increment errors here because URLs are unique and some speakers 
        # genuinely give multiple 'Brief Remarks' on the same day.
    else:
        print("Check 2 passed: No duplicate speeches found based on bank, date, title, and speaker.")
        
    # 3. Check for lingering mojibake
    cur.execute("""
        SELECT COUNT(*) as cnt FROM speeches
        WHERE full_text LIKE '%\ucc3d\x80%' OR title LIKE '%\ucc3d\x80%'
    """)
    mojibake = cur.fetchone()['cnt']
    if mojibake > 0:
        print(f"Error: Found {mojibake} remaining mojibake records.")
        errors += 1
    else:
        print("Check 3 passed: No known mojibake patterns detected.")
        
    # 4. Check members table integrity
    cur.execute("SELECT bank_code, name, COUNT(*) as cnt FROM members GROUP BY bank_code, name HAVING cnt > 1")
    dup_members = cur.fetchall()
    if dup_members:
        print(f"Error: Found {len(dup_members)} duplicate members.")
        errors += 1
    else:
        print("Check 4 passed: No duplicate members.")
        
    conn.close()
    
    if errors > 0:
        print(f"Verification failed with {errors} error(s).")
        sys.exit(1)
    else:
        print("All database integrity checks passed successfully!")

if __name__ == '__main__':
    main()
