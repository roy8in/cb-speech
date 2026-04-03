import sqlite3
import re
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

def fix_boe_dates():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Find BOE speeches that likely have a defaulted date (ending in -01-01)
    speeches = cursor.execute("""
        SELECT id, date, full_text 
        FROM speeches 
        WHERE bank_code = 'BOE' AND date LIKE '%-01-01' AND full_text IS NOT NULL
    """).fetchall()

    fixed_count = 0
    for s in speeches:
        text = s['full_text'][:1500]  # Check the beginning of the text
        
        # Look for dates like "24 January 2019" or "January 24, 2019"
        match = re.search(r'(\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})', text)
        if not match:
            match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', text)
            
        if match:
            try:
                # Try parsing "24 January 2019"
                if match.group(1).isdigit():
                    dt_str = f"{match.group(1)} {match.group(2)} {match.group(3)}"
                else:
                    # Try parsing "January 24 2019"
                    dt_str = f"{match.group(2)} {match.group(1)} {match.group(3)}"
                    
                dt = datetime.strptime(dt_str, '%d %B %Y')
                exact_date = dt.strftime('%Y-%m-%d')
                
                # Ensure the extracted year matches the URL's year to prevent wild guesses
                if exact_date.startswith(s['date'][:4]):
                    cursor.execute("UPDATE speeches SET date = ? WHERE id = ?", (exact_date, s['id']))
                    fixed_count += 1
            except ValueError:
                pass

    conn.commit()
    conn.close()
    print(f"Successfully fixed dates for {fixed_count} BOE speeches.")

if __name__ == "__main__":
    fix_boe_dates()
