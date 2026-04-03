"""
Generates dashboard_data.json for the GitHub Pages dashboard.
Extracts stats, health status, and recent speeches from the SQLite DB.
"""

import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from core.config import config

def update_dashboard():
    if not config.SPEECH_DB_PATH.exists():
        print(f"Error: Database not found at {config.SPEECH_DB_PATH}")
        return

    conn = sqlite3.connect(config.SPEECH_DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        # 1. 은행별 통계
        banks = ['FRB', 'ECB', 'BOE', 'BOJ', 'RBA', 'BOC']
        bank_stats = {bank: 0 for bank in banks}
        rows = conn.execute("SELECT bank_code, COUNT(*) as count FROM speeches GROUP BY bank_code").fetchall()
        for r in rows:
            bank_stats[r['bank_code']] = r['count']
            
        # 1-2. 분석 상태별 통계 (분류 기준 적용)
        status_stats = {
            'scored': 0,
            'no_signal': 0,
            'skipped': 0,
            'pending': 0
        }
        status_rows = conn.execute("SELECT analysis_status, COUNT(*) as count FROM speeches GROUP BY analysis_status").fetchall()
        for r in status_rows:
            if r['analysis_status'] in status_stats:
                status_stats[r['analysis_status']] = r['count']
            
        # 1-1. 월별 은행별 연설 건수 (시계열 차트용)
        # date: YYYY-MM-DD
        time_series_data = {}
        ts_rows = conn.execute("""
            SELECT bank_code, substr(date, 1, 7) as month, COUNT(*) as count 
            FROM speeches 
            WHERE date >= '2008-01-01'  -- 과거 데이터까지 모두 포함
            GROUP BY bank_code, month
            ORDER BY month ASC
        """).fetchall()
        
        for r in ts_rows:
            bank = r['bank_code']
            month = r['month']
            count = r['count']
            if bank not in time_series_data:
                time_series_data[bank] = {}
            time_series_data[bank][month] = count
            
        # 2. 최근 수집 로그 (최근 5건)
        logs = []
        rows = conn.execute("SELECT * FROM collection_logs ORDER BY started_at DESC LIMIT 5").fetchall()
        for r in rows:
            logs.append({
                'started_at': r['started_at'],
                'finished_at': r['finished_at'],
                'status': r['status'],
                'total_new': r['total_new_speeches'],
                'error': r['error_message']
            })
            
        # 3. 최근 연설 (넉넉하게 최근 14일치 탐색)
        # 오늘이 3월 12일이면 3월 5일 이후 데이터를 가져옵니다.
        threshold_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        recent_speeches = []
        rows = conn.execute("""
            SELECT s.bank_code, m.name as speaker, s.title, s.date, s.url 
            FROM speeches s
            LEFT JOIN members m ON s.speaker_id = m.id
            WHERE s.date >= ? 
            ORDER BY s.date DESC, s.fetched_at DESC
            LIMIT 50
        """, (threshold_date,)).fetchall()
        
        for r in rows:
            recent_speeches.append({
                'bank': r['bank_code'],
                'speaker': r['speaker'],
                'title': r['title'],
                'date': r['date'],
                'url': r['url']
            })
            
        # 4. 전체 요약
        total_speeches = conn.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
        
        dashboard_data = {
            'last_updated': datetime.now().isoformat(),
            'total_speeches': total_speeches,
            'bank_stats': bank_stats,
            'status_stats': status_stats,
            'time_series_data': time_series_data,
            'recent_logs': logs,
            'recent_speeches': recent_speeches,
            'health': logs[0]['status'] if logs else 'unknown'
        }
        
        # docs 디렉토리 생성
        config.SPEECH_DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config.SPEECH_DASHBOARD_PATH, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully generated dashboard data at {config.SPEECH_DASHBOARD_PATH}. Found {len(recent_speeches)} recent speeches.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    update_dashboard()
