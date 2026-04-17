
import sys
import os
import re
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from tools.speech_tracker.models import SpeechDB

# Curated list of known members for robust matching
CANONICAL_MEMBERS = {
    'BOJ': [
        'Kazuo Ueda', 'Shinichi Uchida', 'Ryozo Himino', 'Seiji Adachi', 'Asahi Noguchi',
        'Junko Nakagawa', 'Hajime Takata', 'Naoki Tamura', 'Toyoaki Nakamura',
        'Haruhiko Kuroda', 'Masayoshi Amamiya', 'Masazumi Wakatabe', 'Goushi Kataoka',
        'Hitoshi Suzuki', 'Takako Masai', 'Yukitoshi Funo', 'Makoto Sakurai', 'Kikuo Iwata',
        'Hiroshi Nakaso', 'Masaaki Shirakawa', 'Sayuri Shirai', 'Ryuzo Miyao'
    ],
    'BOE': [
        'Andrew Bailey', 'Sarah Breeden', 'Dave Ramsden', 'Clare Lombardelli', 'Huw Pill',
        'Megan Greene', 'Jonathan Haskel', 'Catherine L. Mann', 'Swati Dhingra',
        'Ben Broadbent', 'Jon Cunliffe', 'Silvana Tenreyro', 'Gertjan Vlieghe', 'Mark Carney',
        'Michael Saunders', 'Andy Haldane', 'Andrew Haldane', 'Jan Vlieghe', 'Ian McCafferty',
        'Kristin Forbes', 'Minouche Shafik', 'Paul Fisher', 'Charles Bean', 'Mervyn King',
        'Paul Tucker', 'David Miles', 'Adam Posen', 'Spencer Dale', 'Martin Weale',
        'Charlotte Hogg', 'Nemat Shafik', 'Rachel Lomax', 'John Gieve', 'Kate Barker'
    ],
    'ECB': [
        'Christine Lagarde', 'Luis de Guindos', 'Philip R. Lane', 'Piero Cipollone',
        'Frank Elderson', 'Isabel Schnabel', 'Fabio Panetta', 'Mario Draghi', 'Vítor Constâncio',
        'Benoît Cœuré', 'Sabine Lautenschläger', 'Yves Mersch', 'Peter Praet', 'Jean-Claude Trichet',
        'Lucas Papademos', 'Gertrude Tumpel-Gugerell', 'José Manuel González-Páramo'
    ],
    'FRB': [
        'Jerome H. Powell', 'Jerome Powell', 'Philip N. Jefferson', 'Michael S. Barr',
        'Michelle W. Bowman', 'Lisa D. Cook', 'Christopher J. Waller', 'Adriana D. Kugler',
        'Lael Brainard', 'Janet L. Yellen', 'Janet Yellen', 'Ben S. Bernanke', 'Ben Bernanke',
        'Alan Greenspan', 'Richard H. Clarida', 'Randal K. Quarles', 'Stanley Fischer',
        'Daniel K. Tarullo', 'Sarah Bloom Raskin', 'Jeremy C. Stein', 'Elizabeth A. Duke'
    ],
    'RBA': [
        'Michele Bullock', 'Andrew Hauser', 'Sarah Hunter', 'Christopher Kent', 'Brad Jones',
        'Philip Lowe', 'Guy Debelle', 'Glenn Stevens', 'Andrea Brischetto', 'Ellis Connolly',
        'Marion Kohler', 'Michael Plumb', 'Penelope Smith', 'David Jacobs'
    ],
    'BOC': [
        'Tiff Macklem', 'Carolyn Rogers', 'Sharon Kozicki', 'Nicolas Vincent', 'Rhys Mendes',
        'Stephen S. Poloz', 'Stephen Poloz', 'Carolyn A. Wilkins', 'Carolyn Wilkins',
        'Timothy Lane', 'Lawrence Schembri', 'Paul Beaudry', 'Mark Carney', 'David Dodge'
    ]
}

def repair_all_speakers():
    db = SpeechDB()
    
    # 1. First, create all canonical members if they don't exist
    print("Ensuring canonical members exist...")
    for bank_code, names in CANONICAL_MEMBERS.items():
        for name in names:
            db.get_or_create_member(bank_code, name)

    # 2. Get all speeches with NULL speaker_id
    conn = db._get_conn()
    try:
        speeches = conn.execute("""
            SELECT id, bank_code, title, url, full_text 
            FROM speeches 
            WHERE speaker_id IS NULL AND full_text IS NOT NULL
        """).fetchall()
        speeches_list = [dict(s) for s in speeches]
    finally:
        conn.close()
    
    print(f"Attempting to repair {len(speeches_list)} speeches...")
    
    repaired_count = 0
    
    for s in speeches_list:
        speech_id = s['id']
        bank_code = s['bank_code']
        full_text = s['full_text']
        title = s['title']
        
        found_speaker = None
        
        # Check canonical list first (most reliable)
        # Search in first 1000 chars of full_text and also in title
        search_area = (title + " " + full_text[:1000]).lower()
        
        for name in CANONICAL_MEMBERS.get(bank_code, []):
            if name.lower() in search_area:
                found_speaker = name
                break
        
        # If not found in canonical, try the line-based heuristic (backup)
        if not found_speaker:
            lines = [l.strip() for l in full_text.split('\n') if l.strip()][:10]
            for line in lines:
                # Basic name check: 2-3 words, capitalized
                if 2 <= len(line.split()) <= 3 and re.match(r'^[A-Z][A-Za-z\s\.]+$', line):
                    # Check if it's not a known noise word
                    if not any(noise in line.lower() for noise in ['governor', 'speech', 'transcript', 'bank']):
                        found_speaker = line
                        break

        if found_speaker:
            # Map name to ID
            speaker_id = db.get_or_create_member(bank_code, found_speaker)
            if speaker_id:
                temp_conn = db._get_conn()
                try:
                    temp_conn.execute("UPDATE speeches SET speaker_id = ?, synced_at = NULL WHERE id = ?", (speaker_id, speech_id))
                    temp_conn.commit()
                    repaired_count += 1
                finally:
                    temp_conn.close()
            
    print(f"Repaired {repaired_count} speeches total.")

if __name__ == "__main__":
    repair_all_speakers()
