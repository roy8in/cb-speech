"""
Members 테이블 데이터 정리 스크립트
1. 사람 이름이 아닌 쓰레기 데이터를 status='invalid'로 마킹
2. 관련 speeches의 speaker_id를 올바른 멤버로 재연결 (가능한 경우)
3. 임기 7일 미만의 의심스러운 term_start/term_end를 NULL로 초기화
"""
import sqlite3

DB_PATH = "data/speeches.db"

# 1. 명확히 사람 이름이 아닌 entries (수동 확인 후 확정 목록)
INVALID_MEMBER_IDS = [
    # BOC
    1,    # 'Export Development' — 기관명
    # BOE — 연설 제목, 슬라이드, 트랜스크립트 등
    330,  # 'Achieving a sustainable recovery'
    100,  # 'Anil Kashyap slides'
    84,   # 'Central clearing'
    346,  # 'Charlotte Gerken accompanying slides'
    336,  # 'Dave Ramsden slides'
    345,  # 'Jonathan Haskel accompanying slides'
    342,  # 'Jonathan Haskel slides'
    331,  # 'Mark Carney Press Conference transcript'
    332,  # 'Mark Carney Q&A transcript'
    335,  # 'Mark Carney presentation slides'
    334,  # 'Mark Carney slides'
    333,  # 'Mark Carney transcript'
    350,  # 'Megan Greene transcript'
    337,  # 'Policy Panel'
    98,   # 'Pull, push, pipes'
    79,   # 'Running out of room'
    343,  # 'Silvana Tenreyro accompanying slides'
    347,  # 'Speech annex'
    99,   # 'Stress tests'
    341,  # 'Victoria Cleland: Annex 1'
    # BOJ
    117,  # 'Keynote Speech'
    123,  # '日本語' — 언어 태그
    # RBA
    169,  # 'Assistant  (Financial System)' — 직급
]

# 2. 슬라이드/트랜스크립트 멤버의 speeches를 원래 화자에게 재연결
# { invalid_member_id: correct_member_name }
REMAP_SPEAKERS = {
    334: 'Mark Carney',       # 'Mark Carney slides' -> Mark Carney
    335: 'Mark Carney',       # 'Mark Carney presentation slides' -> Mark Carney
    331: 'Mark Carney',       # 'Mark Carney Press Conference transcript' -> Mark Carney
    332: 'Mark Carney',       # 'Mark Carney Q&A transcript' -> Mark Carney
    333: 'Mark Carney',       # 'Mark Carney transcript' -> Mark Carney
    336: 'Dave Ramsden',      # 'Dave Ramsden slides' -> Dave Ramsden
    346: 'Charlotte Gerken',  # 'Charlotte Gerken accompanying slides' -> Charlotte Gerken
    345: 'Jonathan Haskel',   # 'Jonathan Haskel accompanying slides' -> Jonathan Haskel
    342: 'Jonathan Haskel',   # 'Jonathan Haskel slides' -> Jonathan Haskel
    343: 'Silvana Tenreyro',  # 'Silvana Tenreyro accompanying slides' -> Silvana Tenreyro
    350: 'Megan Greene',      # 'Megan Greene transcript' -> Megan Greene
    341: 'Victoria Cleland',  # 'Victoria Cleland: Annex 1' -> Victoria Cleland
}

def clean_members():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("=== Members 테이블 데이터 정리 ===\n")
    
    # Step 1: Remap speeches from invalid slide/transcript members to real members
    remapped = 0
    for invalid_id, correct_name in REMAP_SPEAKERS.items():
        # Find the correct member's id
        row = conn.execute("SELECT id FROM members WHERE name = ? AND bank_code = 'BOE'", (correct_name,)).fetchone()
        if row:
            correct_id = row['id']
            count = conn.execute("UPDATE speeches SET speaker_id = ? WHERE speaker_id = ?", (correct_id, invalid_id)).rowcount
            remapped += count
            if count > 0:
                print(f"  ✅ Remapped {count} speech(es): '{correct_name} slides/transcript' -> '{correct_name}' (id={correct_id})")
    
    print(f"\n  총 {remapped}개 연설문 재연결 완료.\n")
    
    # Step 2: Mark invalid members
    invalid_count = 0
    for mid in INVALID_MEMBER_IDS:
        row = conn.execute("SELECT name FROM members WHERE id = ?", (mid,)).fetchone()
        if row:
            conn.execute("UPDATE members SET status = 'invalid' WHERE id = ?", (mid,))
            invalid_count += 1
            print(f"  🚫 Invalid: id={mid} name='{row['name']}'")
    
    print(f"\n  총 {invalid_count}개 멤버를 'invalid'로 마킹 완료.\n")
    
    # Step 3: Reset suspicious terms (< 7 days) to NULL
    result = conn.execute("""
        UPDATE members 
        SET term_start = NULL, term_end = NULL
        WHERE term_start IS NOT NULL 
        AND term_end IS NOT NULL
        AND julianday(term_end) - julianday(term_start) < 7
        AND status != 'invalid'
    """)
    print(f"  🔄 임기 7일 미만 의심 데이터 {result.rowcount}건의 term_start/term_end를 NULL로 초기화.\n")
    
    conn.commit()
    
    # Summary
    total_members = conn.execute("SELECT COUNT(*) FROM members").fetchone()[0]
    valid_members = conn.execute("SELECT COUNT(*) FROM members WHERE status != 'invalid'").fetchone()[0]
    print(f"=== 정리 완료 ===")
    print(f"  전체 멤버: {total_members}")
    print(f"  유효 멤버: {valid_members}")
    print(f"  무효(invalid): {total_members - valid_members}")
    
    conn.close()

if __name__ == "__main__":
    clean_members()
