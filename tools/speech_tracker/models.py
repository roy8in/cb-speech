"""
Central Bank Watchtower — Data Models (SQLite Optimized)

Tables:
  - speeches: 원본 연설 데이터 및 메타데이터
  - speeches_fts: 전문 검색(Full-Text Search)을 위한 가상 테이블
  - members: 중앙은행 위원 정보
"""

import sqlite3
import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from core.config import config

def get_db_path():
    config.SPEECH_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(config.SPEECH_DB_PATH)

class SpeechDB:
    BANKS = ('FRB', 'ECB', 'BOE', 'BOJ', 'RBA', 'BOC')

    def __init__(self, db_path=None):
        self.db_path = db_path or get_db_path()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        """데이터베이스 초기화 및 인덱스 설정"""
        conn = self._get_conn()
        try:
            # 1. 메인 연설 테이블
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS speeches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_code TEXT NOT NULL,
                    speaker_id INTEGER,
                    title TEXT NOT NULL,
                    date TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    full_text TEXT,
                    speech_type TEXT DEFAULT 'speech',
                    language TEXT DEFAULT 'en',
                    fetched_at TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    synced_at TEXT,
                    FOREIGN KEY (speaker_id) REFERENCES members (id)
                );

                CREATE TABLE IF NOT EXISTS analysis_results (
                    speech_id INTEGER PRIMARY KEY,
                    stance_score REAL,
                    stance_reason TEXT,
                    keywords TEXT, -- JSON array of {category, detail}
                    main_risk TEXT, -- Primary threat to policy goals
                    analysis_attempts INTEGER DEFAULT 0,
                    analysis_status TEXT DEFAULT 'pending', -- scored, no_signal, skipped, pending
                    analyzed_at TEXT DEFAULT (datetime('now')),
                    synced_at TEXT,
                    FOREIGN KEY (speech_id) REFERENCES speeches (id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    role TEXT,
                    status TEXT DEFAULT 'active',
                    term_start TEXT,
                    term_end TEXT,
                    last_speech_date TEXT,
                    last_verified_at TEXT,
                    last_updated TEXT DEFAULT (datetime('now')),
                    avg_stance_score REAL,
                    synced_at TEXT,
                    UNIQUE(bank_code, name)
                );

                CREATE INDEX IF NOT EXISTS idx_speeches_bank ON speeches(bank_code);
                CREATE INDEX IF NOT EXISTS idx_speeches_date ON speeches(date);
                CREATE INDEX IF NOT EXISTS idx_speeches_speaker ON speeches(speaker_id);
                CREATE INDEX IF NOT EXISTS idx_analysis_status ON analysis_results(analysis_status);
                CREATE INDEX IF NOT EXISTS idx_members_status ON members(status);

                CREATE TABLE IF NOT EXISTS pipeline_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    stage_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_seconds REAL,
                    status TEXT,
                    item_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    details_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id ON pipeline_logs(run_id);
                CREATE INDEX IF NOT EXISTS idx_pipeline_logs_stage_name ON pipeline_logs(stage_name);
            """)
            
            # Migration for existing DBs
            self._migrate_db(conn)
            
            # 2. FTS5 전문 검색 테이블 (SQLite FTS5 모듈 필요)
            try:
                conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS speeches_fts USING fts5(title, full_text, content='speeches', content_rowid='id')")
                # 트리거 생성: 원본 테이블에 데이터 삽입/수정/삭제 시 FTS 테이블 자동 업데이트
                conn.executescript("""
                    CREATE TRIGGER IF NOT EXISTS speeches_ai AFTER INSERT ON speeches BEGIN
                      INSERT INTO speeches_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
                    END;
                    CREATE TRIGGER IF NOT EXISTS speeches_ad AFTER DELETE ON speeches BEGIN
                      INSERT INTO speeches_fts(speeches_fts, rowid, title, full_text) VALUES('delete', old.id, old.title, old.full_text);
                    END;
                    CREATE TRIGGER IF NOT EXISTS speeches_au AFTER UPDATE ON speeches BEGIN
                      INSERT INTO speeches_fts(speeches_fts, rowid, title, full_text) VALUES('delete', old.id, old.title, old.full_text);
                      INSERT INTO speeches_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
                    END;
                """)
            except sqlite3.OperationalError:
                pass
                
            conn.commit()
        finally:
            conn.close()

    def _migrate_db(self, conn):
        """Add missing columns and tables to existing databases."""
        # 1. Migrate members table
        cursor = conn.execute("PRAGMA table_info(members)")
        columns = [row['name'] for row in cursor.fetchall()]
        
        new_member_cols = [
            ('term_start', 'TEXT'),
            ('term_end', 'TEXT'),
            ('last_speech_date', 'TEXT'),
            ('last_verified_at', 'TEXT'),
            ('last_updated', "TEXT DEFAULT (datetime('now'))"),
            ('avg_stance_score', 'REAL'),
            ('synced_at', 'TEXT')
        ]
        
        for col_name, col_type in new_member_cols:
            if col_name not in columns:
                try:
                    conn.execute(f"ALTER TABLE members ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    pass
        
        # 2. Check if we need to split speeches and analysis_results
        cursor = conn.execute("PRAGMA table_info(speeches)")
        speech_cols = [row['name'] for row in cursor.fetchall()]
        
        if 'synced_at' not in speech_cols:
            try:
                conn.execute("ALTER TABLE speeches ADD COLUMN synced_at TEXT")
            except sqlite3.OperationalError:
                pass

        if 'analysis_status' in speech_cols:
            # This database hasn't been split yet.
            try:
                # Copy data if analysis_results is empty or doesn't exist (created in _init_db)
                conn.execute("""
                    INSERT OR REPLACE INTO analysis_results 
                    (speech_id, stance_score, stance_reason, keywords, main_risk, analysis_attempts, analysis_status)
                    SELECT id, stance_score, stance_reason, keywords, main_risk, analysis_attempts, analysis_status
                    FROM speeches
                    WHERE analysis_status IS NOT NULL
                """)
                
                # Remove columns from speeches
                cols_to_remove = [
                    'stance_score', 'stance_reason', 'keywords', 'main_risk', 
                    'analysis_attempts', 'analysis_status'
                ]
                conn.execute("DROP INDEX IF EXISTS idx_speeches_status")
                for col in cols_to_remove:
                    if col in speech_cols:
                        conn.execute(f"ALTER TABLE speeches DROP COLUMN {col}")
            except sqlite3.OperationalError:
                pass

        # 3. Migrate analysis_results table — add synced_at column
        cursor = conn.execute("PRAGMA table_info(analysis_results)")
        ar_cols = [row['name'] for row in cursor.fetchall()]
        if 'synced_at' not in ar_cols:
            try:
                conn.execute("ALTER TABLE analysis_results ADD COLUMN synced_at TEXT")
            except sqlite3.OperationalError:
                pass

        # 4. Pipeline logs for stage-by-stage run tracking
        cursor = conn.execute("PRAGMA table_info(pipeline_logs)")
        pipeline_cols = [row['name'] for row in cursor.fetchall()]
        if not pipeline_cols:
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        stage_name TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        duration_seconds REAL,
                        status TEXT,
                        item_count INTEGER DEFAULT 0,
                        error_message TEXT,
                        details_json TEXT
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id ON pipeline_logs(run_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_stage_name ON pipeline_logs(stage_name)")
            except sqlite3.OperationalError:
                pass
        elif 'duration_seconds' not in pipeline_cols:
            try:
                conn.execute("ALTER TABLE pipeline_logs ADD COLUMN duration_seconds REAL")
            except sqlite3.OperationalError:
                pass

    def log_pipeline_step(self, run_id, stage_name, started_at, finished_at, status, item_count=0, error_msg=None, details=None):
        """Record a single pipeline stage in the dedicated step log table."""
        conn = self._get_conn()
        try:
            duration_seconds = None
            try:
                if started_at and finished_at:
                    duration_seconds = (
                        datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
                    ).total_seconds()
            except Exception:
                duration_seconds = None

            conn.execute("""
                INSERT INTO pipeline_logs
                (run_id, stage_name, started_at, finished_at, duration_seconds, status, item_count, error_message, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                stage_name,
                started_at,
                finished_at,
                duration_seconds,
                status,
                item_count,
                error_msg,
                json.dumps(details) if details is not None else None,
            ))
            conn.commit()
        finally:
            conn.close()

    def get_or_create_member(self, bank_code, name, role=None, status='active'):
        """회원 ID를 반환하거나 없으면 생성 (정보 업데이트 포함)"""
        if not name:
            return None
        conn = self._get_conn()
        try:
            cursor = conn.execute("SELECT id, role, status FROM members WHERE bank_code = ? AND name = ?", (bank_code, name))
            row = cursor.fetchone()
            if row:
                # Update role if provided and different
                if (role and row['role'] != role) or (status != row['status']):
                    conn.execute("""
                        UPDATE members 
                        SET role = COALESCE(?, role), status = ?, last_updated = datetime('now')
                        WHERE id = ?
                    """, (role, status, row['id']))
                    conn.commit()
                return row['id']
            
            cursor = conn.execute("""
                INSERT INTO members (bank_code, name, role, status, last_updated) 
                VALUES (?, ?, ?, ?, datetime('now'))
            """, (bank_code, name, role, status))
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_member_official(self, bank_code, name, **kwargs):
        """공식 명단 확인 후 위원 정보 업데이트"""
        conn = self._get_conn()
        try:
            kwargs['last_verified_at'] = datetime.now().strftime('%Y-%m-%d')
            kwargs['last_updated'] = datetime.now().isoformat()
            kwargs['status'] = 'active' # If they are in the official list, they are active
            
            # Build dynamic SQL
            cols = []
            vals = []
            for k, v in kwargs.items():
                cols.append(f"{k} = ?")
                vals.append(v)
            
            sql = f"UPDATE members SET {', '.join(cols)} WHERE bank_code = ? AND name = ?"
            vals.extend([bank_code, name])
            
            cursor = conn.execute(sql, vals)
            if cursor.rowcount == 0:
                # Member not in DB yet, create
                cols = ['bank_code', 'name'] + list(kwargs.keys())
                placeholders = ', '.join(['?'] * len(cols))
                vals = [bank_code, name] + list(kwargs.values())
                conn.execute(f"INSERT INTO members ({', '.join(cols)}) VALUES ({placeholders})", vals)
            
            conn.commit()
        finally:
            conn.close()

    def mark_missing_members_retired(self, bank_code, current_member_names):
        """공식 명단에 없는 위원을 'retired'로 변경"""
        if not current_member_names:
            return 0
            
        conn = self._get_conn()
        try:
            # Mark as retired if they were 'active' but not in the new list
            placeholders = ', '.join(['?'] * len(current_member_names))
            sql = f"""
                UPDATE members 
                SET status = 'retired', 
                    term_end = COALESCE(term_end, date('now')),
                    last_updated = datetime('now')
                WHERE bank_code = ? 
                AND status = 'active'
                AND name NOT IN ({placeholders})
            """
            params = [bank_code] + list(current_member_names)
            cursor = conn.execute(sql, params)
            count = cursor.rowcount
            conn.commit()
            return count
        finally:
            conn.close()

    def get_unsynced_members(self, limit=100):
        """동기화되지 않은 위원 데이터 조회"""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT * FROM members 
                WHERE synced_at IS NULL OR last_updated > synced_at
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def mark_members_as_synced(self, member_ids):
        """위원 데이터를 동기화 완료로 표시"""
        if not member_ids:
            return
        conn = self._get_conn()
        try:
            placeholders = ', '.join(['?'] * len(member_ids))
            conn.execute(f"UPDATE members SET synced_at = datetime('now') WHERE id IN ({placeholders})", member_ids)
            conn.commit()
        finally:
            conn.close()

    def insert_speech(self, bank_code, speaker, title, date, url, full_text=None, speech_type='speech', language='en'):
        """새 연설 삽입 및 위원의 마지막 연설일 갱신"""
        speaker_id = self.get_or_create_member(bank_code, speaker)
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO speeches 
                (bank_code, speaker_id, title, date, url, full_text, speech_type, language, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank_code, speaker_id, title, date, url, full_text, speech_type, language, datetime.now().isoformat()))
            
            if cursor.rowcount > 0 and speaker_id:
                # Update member's last speech date
                conn.execute("""
                    UPDATE members 
                    SET last_speech_date = MAX(COALESCE(last_speech_date, ''), ?),
                        last_updated = datetime('now')
                    WHERE id = ?
                """, (date, speaker_id))
            
            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        finally:
            conn.close()

    def get_incomplete_speeches(self, bank_code=None, days_limit=30):
        """내용이 부실한 최근 연설 목록 조회 (너무 오래된 것은 무시)"""
        conn = self._get_conn()
        try:
            # Only refresh speeches from the last N days to avoid infinite loops on old, 
            # naturally short speeches (like slides or short statements).
            query = """
                SELECT id, url, title, date, fetched_at 
                FROM speeches 
                WHERE (full_text IS NULL OR length(full_text) < 500 OR full_text LIKE '%to be published%')
                AND date <= date('now')
                AND date >= date('now', ?)
            """
            params = [f'-{days_limit} days']
            if bank_code:
                query += " AND bank_code = ?"
                params.append(bank_code)
            
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_speech_content(self, speech_id, full_text, exact_date=None):
        """연설 본문 및 날짜 업데이트"""
        conn = self._get_conn()
        try:
            if exact_date:
                conn.execute("UPDATE speeches SET full_text = ?, date = ?, synced_at = NULL WHERE id = ?", (full_text, exact_date, speech_id))
            else:
                conn.execute("UPDATE speeches SET full_text = ?, synced_at = NULL WHERE id = ?", (full_text, speech_id))
            conn.commit()
        finally:
            conn.close()

    def get_unsynced_speeches(self, limit=100):
        """동기화되지 않은 연설 데이터 조회"""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT s.*, m.name as speaker, ar.stance_score, ar.stance_reason, ar.keywords, ar.main_risk
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE s.synced_at IS NULL
                ORDER BY s.date DESC
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def mark_as_synced(self, speech_ids):
        """연설 데이터를 동기화 완료로 표시"""
        if not speech_ids:
            return
        conn = self._get_conn()
        try:
            placeholders = ', '.join(['?'] * len(speech_ids))
            conn.execute(f"UPDATE speeches SET synced_at = datetime('now') WHERE id IN ({placeholders})", speech_ids)
            conn.commit()
        finally:
            conn.close()

    def get_unsynced_analysis(self, limit=100):
        """동기화되지 않은 분석 결과 조회"""
        conn = self._get_conn()
        try:
            # speeches.url을 외래키 대신 사용하여 PostgreSQL에서 매칭
            rows = conn.execute("""
                SELECT s.url, ar.*
                FROM analysis_results ar
                JOIN speeches s ON ar.speech_id = s.id
                WHERE ar.synced_at IS NULL
                AND ar.analysis_status IN ('scored', 'no_signal')
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def mark_analysis_as_synced(self, speech_ids):
        """분석 결과를 동기화 완료로 표시"""
        if not speech_ids:
            return
        conn = self._get_conn()
        try:
            placeholders = ', '.join(['?'] * len(speech_ids))
            conn.execute(f"UPDATE analysis_results SET synced_at = datetime('now') WHERE speech_id IN ({placeholders})", speech_ids)
            conn.commit()
        finally:
            conn.close()

    def get_existing_urls(self, bank_code):
        """특정 은행의 이미 수집된 URL 목록 조회"""
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT url FROM speeches WHERE bank_code = ?", (bank_code,)).fetchall()
            return {r['url'] for r in rows}
        finally:
            conn.close()

    def search_speeches(self, keyword):
        """FTS5를 이용한 초고속 키워드 검색"""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT s.bank_code, s.date, m.name as speaker, s.title 
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                JOIN speeches_fts f ON s.id = f.rowid
                WHERE speeches_fts MATCH ?
                ORDER BY rank
            """, (keyword,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_stats(self):
        conn = self._get_conn()
        try:
            stats = {}
            # Count per bank
            rows = conn.execute("""
                SELECT s.bank_code, 
                       COUNT(s.id) as total,
                       SUM(CASE WHEN ar.analysis_status IN ('scored', 'no_signal') THEN 1 ELSE 0 END) as analyzed
                FROM speeches s
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                GROUP BY s.bank_code
            """).fetchall()
            
            for r in rows:
                stats[r['bank_code']] = {
                    'total_speeches': r['total'],
                    'analyzed': r['analyzed']
                }
            
            # Grand total
            total = conn.execute("SELECT COUNT(*) as cnt FROM speeches").fetchone()
            stats['total'] = total['cnt']
            return stats
        finally:
            conn.close()

    def update_all_member_stances(self):
        """
        Calculate weighted average of stance scores for all members.
        Speeches within 90 days get weight=1.0
        Within 180 days get weight=0.5
        Within 365 days get weight=0.2
        Older get weight=0.05
        """
        conn = self._get_conn()
        try:
            members = conn.execute("""
                SELECT DISTINCT m.id 
                FROM members m
                JOIN speeches s ON m.id = s.speaker_id
                JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE ar.stance_score IS NOT NULL
            """).fetchall()
            
            for m in members:
                member_id = m['id']
                speeches = conn.execute("""
                    SELECT ar.stance_score, s.date
                    FROM speeches s
                    JOIN analysis_results ar ON s.id = ar.speech_id
                    WHERE s.speaker_id = ? AND ar.stance_score IS NOT NULL
                    ORDER BY s.date DESC
                """, (member_id,)).fetchall()
                
                total_weight = 0.0
                total_score = 0.0
                current_time = datetime.now()
                
                for s in speeches:
                    try:
                        s_date = datetime.strptime(s['date'][:10], '%Y-%m-%d')
                    except ValueError:
                        continue
                        
                    age_days = (current_time - s_date).days
                    
                    if age_days <= 90:
                        weight = 1.0
                    elif age_days <= 180:
                        weight = 0.5
                    elif age_days <= 365:
                        weight = 0.2
                    else:
                        weight = 0.05
                        
                    total_weight += weight
                    total_score += s['stance_score'] * weight
                
                if total_weight > 0:
                    avg_score = total_score / total_weight
                    conn.execute("UPDATE members SET avg_stance_score = ?, synced_at = NULL WHERE id = ?", (avg_score, member_id))
            
            conn.commit()
        finally:
            conn.close()

if __name__ == '__main__':
    db = SpeechDB()
    print(f"Database initialized. Total speeches: {db.get_stats().get('total', 0)}")
