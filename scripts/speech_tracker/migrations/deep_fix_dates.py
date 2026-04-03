import sqlite3
import re
import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
}

def clean_text(text):
    if not text: return ""
    # Remove weird encoding artifacts from BOJ
    return text.replace('', '').replace('ζц', '')

def extract_date(text, bank_code):
    if not text: return None
    text = clean_text(text[:2000])
    
    if bank_code == 'BOJ':
        # Pattern: Jan. 31, 2025 or January 31, 2025
        m = re.search(r'([A-Z][a-z]+)\.?\s*(\d{1,2}),\s*(\d{4})', text)
        if m:
            month_str, day, year = m.groups()
            month = MONTHS.get(month_str[:3].title())
            if month:
                return f"{year}-{int(month):02d}-{int(day):02d}"
    
    if bank_code == 'BOE':
        # Pattern: 24 January 2019 or Published on 24 Jan 2019
        m = re.search(r'(\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})', text)
        if m:
            day, month_str, year = m.groups()
            month = MONTHS.get(month_str.title()) or MONTHS.get(month_str[:3].title())
            if month:
                return f"{year}-{int(month):02d}-{int(day):02d}"
        
        # Pattern: January 24, 2019
        m = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})', text)
        if m:
            month_str, day, year = m.groups()
            month = MONTHS.get(month_str.title()) or MONTHS.get(month_str[:3].title())
            if month:
                return f"{year}-{int(month):02d}-{int(day):02d}"
                
    return None

def fix_all_dates():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Fix BOE and BOJ January spikes
    print("Fixing BOE and BOJ dates from text...")
    rows = cursor.execute("""
        SELECT id, bank_code, full_text, date 
        FROM speeches 
        WHERE (bank_code IN ('BOE', 'BOJ')) 
        AND (date LIKE '%-01-01' OR date LIKE '%-50-%')
    """).fetchall()

    fixed = 0
    for r in rows:
        exact_date = extract_date(r['full_text'], r['bank_code'])
        if exact_date:
            # Ensure year roughly matches to avoid wrong extraction
            if exact_date[:4] == r['date'][:4] or r['date'].find('-50-') != -1:
                cursor.execute("UPDATE speeches SET date = ? WHERE id = ?", (exact_date, r['id']))
                fixed += 1

    print(f"Updated {fixed} dates from text.")

    # 2. Fix specific invalid dates like '2019-50-01'
    print("Cleaning up invalid date formats...")
    cursor.execute("UPDATE speeches SET date = substr(date, 1, 4) || '-01-01' WHERE date LIKE '%-50-%'")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_all_dates()
