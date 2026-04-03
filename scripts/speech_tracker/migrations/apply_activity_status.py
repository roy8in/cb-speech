import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

# Fix path to resolve correctly from the project root
BASE_DIR = Path(__file__).resolve().parents[3]
DB_PATH = BASE_DIR / "data" / "speech_tracker" / "speeches.db"

def apply_activity_based_status(days_threshold=365):
    """
    Marks members as 'retired' if they haven't given a speech in `days_threshold` days.
    Also ensures term_end is populated appropriately.
    """
    if not DB_PATH.exists():
        print(f"Error: DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Define the cutoff date
    cutoff_date = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d')
    print(f"Threshold: {days_threshold} days (Cutoff: {cutoff_date})")

    # 1. Update status to 'retired' for those with last_speech_date < cutoff_date
    #    If term_end is null, set it to last_speech_date
    cursor.execute("""
        UPDATE members 
        SET status = 'retired',
            term_end = COALESCE(term_end, last_speech_date),
            last_updated = datetime('now')
        WHERE last_speech_date < ? 
        AND status = 'active'
    """, (cutoff_date,))
    
    retired_count = cursor.rowcount
    print(f"Marked {retired_count} members as retired based on inactivity.")

    # 2. Re-activate members who might have given a recent speech but were marked retired
    cursor.execute("""
        UPDATE members 
        SET status = 'active',
            term_end = NULL,
            last_updated = datetime('now')
        WHERE last_speech_date >= ? 
        AND status = 'retired'
    """, (cutoff_date,))
    
    reactivated_count = cursor.rowcount
    print(f"Re-activated {reactivated_count} members based on recent activity.")

    conn.commit()
    
    # 3. Print current stats
    print("\nCurrent Member Stats:")
    rows = cursor.execute("""
        SELECT bank_code, status, COUNT(*) as count 
        FROM members 
        GROUP BY bank_code, status
        ORDER BY bank_code, status
    """).fetchall()
    
    for r in rows:
        print(f"  {r[0]} - {r[1]}: {r[2]}")

    conn.close()

if __name__ == "__main__":
    apply_activity_based_status()
