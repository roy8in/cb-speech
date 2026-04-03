import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

SPEAKER_MAP = {
    'Barr': 'Michael S. Barr',
    'Bowman': 'Michelle W. Bowman',
    'Brainard': 'Lael Brainard',
    'Clarida': 'Richard H. Clarida',
    'Cook': 'Lisa D. Cook',
    'Jefferson': 'Philip N. Jefferson',
    'Kugler': 'Adriana D. Kugler',
    'Miran': 'Stephen I. Miran',
    'Powell': 'Jerome H. Powell',
    'Quarles': 'Randal K. Quarles',
    'Waller': 'Christopher J. Waller',
    'Yellen': 'Janet L. Yellen',
    'Bernanke': 'Ben S. Bernanke',
    'Tarullo': 'Daniel K. Tarullo',
}

def fix_frb_names():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    for short_name, full_name in SPEAKER_MAP.items():
        # Check if short_name exists
        short_member = cursor.execute("SELECT id FROM members WHERE bank_code='FRB' AND name=?", (short_name,)).fetchone()
        if not short_member:
            continue
            
        short_id = short_member['id']
        
        # Check if full_name already exists
        full_member = cursor.execute("SELECT id FROM members WHERE bank_code='FRB' AND name=?", (full_name,)).fetchone()
        
        if full_member:
            full_id = full_member['id']
            print(f"Merging {short_name} (ID: {short_id}) -> {full_name} (ID: {full_id})")
            
            # Update speeches to point to the full name ID
            cursor.execute("UPDATE speeches SET speaker_id = ? WHERE speaker_id = ?", (full_id, short_id))
            
            # Delete the old short name record
            cursor.execute("DELETE FROM members WHERE id = ?", (short_id,))
        else:
            print(f"Updating {short_name} -> {full_name} (No merge needed)")
            cursor.execute("UPDATE members SET name = ? WHERE id = ?", (full_name, short_id))

    conn.commit()
    conn.close()
    print("FRB names updated successfully.")

if __name__ == "__main__":
    fix_frb_names()
