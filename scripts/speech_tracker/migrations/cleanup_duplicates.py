import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.models import SpeechDB
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup():
    db = SpeechDB()
    conn = db._get_conn()
    
    # 1. 논리적 중복 그룹 찾기 (은행, 제목, 화자, 날짜 동일)
    # 본문(full_text)이 가장 긴 레코드를 남기기 위해 정렬하여 처리
    query = '''
        SELECT bank_code, title, speaker_id, date, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM speeches
        GROUP BY bank_code, title, speaker_id, date
        HAVING cnt > 1
    '''
    duplicates = conn.execute(query).fetchall()
    logger.info(f"Found {len(duplicates)} groups of logical duplicates.")
    
    deleted_total = 0
    for group in duplicates:
        ids = [int(i) for i in group['ids'].split(',')]
        
        # 각 ID별 본문 길이를 확인하여 가장 긴 것을 유지
        placeholders = ', '.join(['?'] * len(ids))
        rows = conn.execute(f"SELECT id, LENGTH(COALESCE(full_text, '')) as lang FROM speeches WHERE id IN ({placeholders})", ids).fetchall()
        
        # 길이 기준 내림차순 정렬 (본문이 긴 것이 위로)
        rows = sorted(rows, key=lambda x: x['lang'], reverse=True)
        keep_id = rows[0]['id']
        delete_ids = [r['id'] for r in rows[1:]]
        
        if delete_ids:
            del_placeholders = ', '.join(['?'] * len(delete_ids))
            conn.execute(f"DELETE FROM speeches WHERE id IN ({del_placeholders})", delete_ids)
            deleted_total += len(delete_ids)
            
    # 2. FTS 테이블 Rebuild
    if deleted_total > 0:
        logger.info(f"Deleted {deleted_total} redundant records.")
        try:
            conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('rebuild')")
            logger.info("FTS search index rebuilt.")
        except:
            pass
            
    conn.commit()
    conn.close()
    logger.info("Cleanup complete.")

if __name__ == '__main__':
    cleanup()
