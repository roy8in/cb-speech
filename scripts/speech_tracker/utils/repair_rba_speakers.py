
import sys
import os
import re
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.models import SpeechDB

def repair_rba_speakers():
    db = SpeechDB()
    conn = db._get_conn()
    
    # 1. Get all RBA speeches with NULL speaker_id
    try:
        speeches = conn.execute("""
            SELECT id, title, url, full_text 
            FROM speeches 
            WHERE bank_code = 'RBA' AND speaker_id IS NULL AND full_text IS NOT NULL
        """).fetchall()
        speeches_list = [dict(s) for s in speeches]
    finally:
        conn.close()
    
    print(f"Found {len(speeches_list)} RBA speeches with missing speaker_id")
    
    repaired_count = 0
    
    for s in speeches_list:
        speech_id = s['id']
        full_text = s['full_text']
        
        # Try to extract speaker name from first 10 lines
        lines = [l.strip() for l in full_text.split('\n') if l.strip()][:10]
        if not lines:
            continue
            
        speaker = None
        
        # Pattern 1: Q&A Transcript often has:
        # Transcript of Question & Answer Session
        # [Title]
        # [Speaker Name]
        # [Role]
        if "Transcript" in lines[0] and len(lines) >= 3:
            for idx in [2, 3]:
                if idx < len(lines):
                    candidate = lines[idx]
                    if 2 <= len(candidate.split()) <= 4 and re.match(r'^[A-Za-z\s\.]+$', candidate):
                        speaker = candidate
                        break
        
        # Pattern 2: Regular speech often has:
        # [Title]
        # [Speaker Name]
        # [Role]
        elif len(lines) >= 2:
            for idx in [1, 2]:
                if idx < len(lines):
                    candidate = lines[idx]
                    if 2 <= len(candidate.split()) <= 4 and re.match(r'^[A-Za-z\s\.]+$', candidate):
                        if candidate.lower() not in ['governor', 'deputy governor', 'assistant governor', 'senior officer', 'audio', 'transcript', 'speech']:
                            speaker = candidate
                            break

        if speaker:
            speaker = " ".join(speaker.split())
            if speaker.lower() in ['governor', 'deputy governor', 'assistant governor', 'senior officer', 'download', 'audio', 'transcript']:
                speaker = None
                
        if speaker:
            # Get or create member
            # This method opens its own connection
            speaker_id = db.get_or_create_member('RBA', speaker)
            
            if speaker_id:
                # Need a temporary connection for the update
                temp_conn = db._get_conn()
                try:
                    temp_conn.execute("UPDATE speeches SET speaker_id = ?, synced_at = NULL WHERE id = ?", (speaker_id, speech_id))
                    temp_conn.commit()
                    repaired_count += 1
                finally:
                    temp_conn.close()
            
    print(f"Repaired {repaired_count} RBA speeches.")

if __name__ == "__main__":
    repair_rba_speakers()
