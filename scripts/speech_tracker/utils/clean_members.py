
import sys
import re
import sqlite3
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from tools.speech_tracker.models import SpeechDB

def clean_members():
    db = SpeechDB()
    conn = db._get_conn()
    
    # 1. Get all members
    members = conn.execute("SELECT id, bank_code, name FROM members").fetchall()
    
    blacklist = [
        'slides', 'transcript', 'appendix', 'annex', 'foreword', 'lead comment', 
        'comments on', 'comments given by', 'press conference', 'presentation',
        'technical appendix', 'accompanying slides', 'available as', 'accessible fx',
        'achieving a sustainable', 'an evolving', 'finding the right', 'five years of',
        'from design to', 'global action', 'inclusive capitalism', 'inflation persistence',
        'investment association', 'looking through', 'macroprudential policy',
        'measuring recession', 'one bank research', 'protecting economic',
        'prudential regulation', 'pull, push, pipes', 'returning inflation',
        'running out of room', 'speaker requests', 'speech annex', 'stress tests',
        'tails of the unexpected', 'the future of work', 'the grand unifying',
        'update and outlook', 'wwhy ssttaggffllaattiioonn', 'aiming at', 'article contributed',
        'at the turning point', 'cashless payment', 'digital innovation', 'germany and japan',
        'how to overcome', 'keynote speech', 'list by year', 'overcoming deflation',
        'path toward', 'realizing the asian', 'revisiting qqe', 'the future of money',
        'toward overcoming', 'two years under', '日本語', 'assistant (financial system)',
        'the australian', 'interest rate benchmarks', 'renminbi internationalisation',
        'buffers and options', 'inflation and monetary policy', 'an accounting',
        'resilience and ongoing', 'after the boom', 'the long run', 'fundamentals and flexibility',
        'international and domestic', 'issues in economic', 'managing two transitions',
        'building on strong', 'economic possibilities', 'issues in payments', 'the economic scene',
        'challenges for economic', 'economic update', 'economic conditions', 'the economic outlook',
        'anniversary event', 'light is therefore colour', 'for immediate release', 'speech by',
        'remarks by', 'lecture by', 'address by', 'keynote by'
    ]

    # Pre-compiled regex for normalization
    title_regex = re.compile(r'^(Sir|Dame|Professor|Dr\.|Governor|Deputy Governor|Assistant Governor|Executive Director)\s+', re.I)

    # Dictionary to store canonical names: (bank_code, normalized_name) -> member_id
    canonical_members = {}
    # Dictionary to map old IDs to new IDs
    id_map = {}
    # IDs to delete
    to_delete = []

    print(f"Processing {len(members)} members...")

    # First pass: identify normalization and merges
    for m in members:
        m_id = m['id']
        bank_code = m['bank_code']
        original_name = m['name']
        
        # Check blacklist
        lower_name = original_name.lower()
        if any(item in lower_name for item in blacklist) or len(original_name) < 3:
            print(f"Blacklisting: {original_name}")
            to_delete.append(m_id)
            continue
            
        # Normalize name
        name = original_name.strip()
        name = title_regex.sub('', name)
        
        # Bank-specific normalization
        if bank_code == 'BOJ':
            parts = name.split()
            if len(parts) == 2:
                if parts[0].isupper() and not parts[1].isupper():
                    name = f"{parts[1]} {parts[0].capitalize()}"
                elif parts[1].isupper() and not parts[0].isupper():
                    name = f"{parts[0]} {parts[1].capitalize()}"
                else:
                    name = " ".join([p.capitalize() for p in parts])
        elif bank_code == 'BOE':
            if name == "Andy Haldane" or name == "Andrew G Haldane": name = "Andrew Haldane"
            elif name == "Charlie Bean": name = "Charles Bean"
            elif name == "Andrew Sentence": name = "Andrew Sentance"
            elif name == "Silvan Tenreyro": name = "Silvana Tenreyro"
            elif name == "PPaauull FFiisshheerr": name = "Paul Fisher"
            elif name == "MARK CARNEY": name = "Mark Carney"
            elif name == "MERVYN KING": name = "Mervyn King"
            elif name == "PAUL TUCKER": name = "Paul Tucker"
            name = re.sub(r'\s*\(\w+\s+\d{4}\)', '', name)
            name = re.sub(r'\s+slides$', '', name, flags=re.I)
            
        name = " ".join([p.capitalize() for p in name.split()])
        
        key = (bank_code, name)
        if key in canonical_members:
            winner_id = canonical_members[key]
            id_map[m_id] = winner_id
            to_delete.append(m_id)
            print(f"Merging: '{original_name}' -> '{name}'")
        else:
            canonical_members[key] = m_id
            if name != original_name:
                print(f"To Normalize: '{original_name}' -> '{name}'")
                # We can't update yet because of unique constraints
                # We'll handle this in a second pass

    # 2. Update speeches with merged IDs
    print(f"Updating speeches with merged IDs...")
    for old_id, new_id in id_map.items():
        conn.execute("UPDATE speeches SET speaker_id = ? WHERE speaker_id = ?", (new_id, old_id))

    # 3. Handle blacklisted members in speeches (set to NULL)
    print(f"Nullifying speeches linked to blacklisted members...")
    blacklisted_truly = [m_id for m_id in to_delete if m_id not in id_map]
    if blacklisted_truly:
        placeholders = ', '.join(['?'] * len(blacklisted_truly))
        conn.execute(f"UPDATE speeches SET speaker_id = NULL WHERE speaker_id IN ({placeholders})", blacklisted_truly)

    # 4. Delete bad/duplicate members
    print(f"Deleting bad member records...")
    if to_delete:
        placeholders = ', '.join(['?'] * len(to_delete))
        conn.execute(f"DELETE FROM members WHERE id IN ({placeholders})", to_delete)

    # 5. Final pass: Update remaining names to normalized forms
    print(f"Applying final name normalizations...")
    for (bank_code, name), m_id in canonical_members.items():
        conn.execute("UPDATE members SET name = ? WHERE id = ?", (name, m_id))

    conn.commit()
    conn.close()
    print("Cleanup complete.")

if __name__ == "__main__":
    clean_members()
