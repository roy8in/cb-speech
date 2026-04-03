import sqlite3
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

def fix_rba_dates():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Find RBA speeches with missing dates
    speeches = cursor.execute("""
        SELECT id, url 
        FROM speeches 
        WHERE bank_code = 'RBA' AND (date IS NULL OR date = '')
    """).fetchall()

    print(f"Found {len(speeches)} RBA speeches with missing dates.")
    
    fixed_count = 0
    for s in speeches:
        url = s['url']
        # Older pattern: DDMMYY (e.g., 160813) - flexible for suffixes
        date_match_old = re.search(r'-(\d{2})(\d{2})(\d{2})[-.]', url)
        if date_match_old:
            dd, mm, yy = date_match_old.groups()
            year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
            exact_date = f"{year}-{mm}-{dd}"
            
            cursor.execute("UPDATE speeches SET date = ? WHERE id = ?", (exact_date, s['id']))
            fixed_count += 1

    conn.commit()
    conn.close()
    print(f"Successfully fixed {fixed_count} RBA speech dates.")

if __name__ == "__main__":
    fix_rba_dates()
