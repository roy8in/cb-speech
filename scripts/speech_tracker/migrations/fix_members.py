
import sqlite3
from pathlib import Path
import re
import os

# Get absolute path to the DB
BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

def fix_member_data():
    if not DB_PATH.exists():
        print(f"Error: DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print(f"Using DB: {DB_PATH}")
    print("Populating last_speech_date and term_start from existing speeches...")
    
    # 1. Get min/max speech dates for each member
    query = """
        SELECT speaker_id, MIN(date) as first_date, MAX(date) as last_date
        FROM speeches
        WHERE speaker_id IS NOT NULL
        GROUP BY speaker_id
    """
    rows = cursor.execute(query).fetchall()
    
    for row in rows:
        cursor.execute("""
            UPDATE members 
            SET term_start = COALESCE(term_start, ?),
                last_speech_date = ?
            WHERE id = ?
        """, (row['first_date'], row['last_date'], row['speaker_id']))

    # 2. Try to extract roles from speech titles if role is missing
    print("Extracting roles from speech titles...")
    members = cursor.execute("SELECT id, name, bank_code FROM members WHERE role IS NULL OR role = ''").fetchall()
    
    for m in members:
        # Find latest speech title for this member
        speech = cursor.execute("""
            SELECT title FROM speeches 
            WHERE speaker_id = ? 
            ORDER BY date DESC LIMIT 1
        """, (m['id'],)).fetchone()
        
        if speech:
            title = speech['title']
            role = None
            if "Governor" in title:
                role = "Governor"
            elif "Chair" in title:
                role = "Chair"
            elif "President" in title:
                role = "President"
            elif "Board Member" in title:
                role = "Board Member"
            
            if role:
                cursor.execute("UPDATE members SET role = ? WHERE id = ?", (role, m['id']))

    # 3. Clean up BOE members
    print("Cleaning up member names...")
    boe_members = cursor.execute("SELECT id, name FROM members WHERE bank_code = 'BOE'").fetchall()
    for bm in boe_members:
        if '(' in bm['name']:
            new_name = bm['name'].split('(')[0].strip()
            exists = cursor.execute("SELECT id FROM members WHERE name = ? AND bank_code = 'BOE' AND id != ?", (new_name, bm['id'])).fetchone()
            if exists:
                cursor.execute("UPDATE speeches SET speaker_id = ? WHERE speaker_id = ?", (exists['id'], bm['id']))
                cursor.execute("DELETE FROM members WHERE id = ?", (bm['id'],))
            else:
                cursor.execute("UPDATE members SET name = ? WHERE id = ?", (new_name, bm['id']))

    conn.commit()
    conn.close()
    print("Data correction complete.")

if __name__ == "__main__":
    fix_member_data()
