"""SQLite data model for central-bank speeches."""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.config import config


def utc_now_iso():
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def get_db_path():
    config.SPEECH_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(config.SPEECH_DB_PATH)


def backup_sqlite_database(db_path=None):
    """Create one consistent SQLite snapshot per UTC date."""
    source_path = Path(db_path or get_db_path())
    if not source_path.exists():
        return None

    backup_dir = source_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    date_label = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    backup_path = backup_dir / (
        f"{source_path.stem}_{date_label}{source_path.suffix}"
    )
    if backup_path.exists():
        return backup_path

    source = sqlite3.connect(source_path)
    target = sqlite3.connect(backup_path)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()
    return backup_path


class SpeechDB:
    BANKS = ("FRB", "ECB", "BOE", "BOJ", "RBA", "BOC")

    def __init__(self, db_path=None):
        self.db_path = str(db_path or get_db_path())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.backup_path = backup_sqlite_database(self.db_path)
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        """Initialize tables and apply safe additive migrations."""
        conn = self._get_conn()
        try:
            conn.executescript(
                """
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
                    created_at TEXT,
                    updated_at TEXT,
                    FOREIGN KEY (speaker_id) REFERENCES members (id)
                );

                CREATE TABLE IF NOT EXISTS analysis_results (
                    speech_id INTEGER PRIMARY KEY,
                    stance_score REAL,
                    stance_reason TEXT,
                    keywords TEXT,
                    main_risk TEXT,
                    analysis_attempts INTEGER DEFAULT 0,
                    analysis_status TEXT DEFAULT 'pending',
                    analyzed_at TEXT,
                    model_name TEXT,
                    analysis_version TEXT,
                    FOREIGN KEY (speech_id) REFERENCES speeches (id)
                        ON DELETE CASCADE
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
                    last_updated TEXT,
                    avg_stance_score REAL,
                    UNIQUE(bank_code, name)
                );

                CREATE INDEX IF NOT EXISTS idx_speeches_bank
                    ON speeches(bank_code);
                CREATE INDEX IF NOT EXISTS idx_speeches_date
                    ON speeches(date);
                CREATE INDEX IF NOT EXISTS idx_speeches_speaker
                    ON speeches(speaker_id);
                CREATE INDEX IF NOT EXISTS idx_analysis_status
                    ON analysis_results(analysis_status);
                CREATE INDEX IF NOT EXISTS idx_members_status
                    ON members(status);

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

                CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id
                    ON pipeline_logs(run_id);
                CREATE INDEX IF NOT EXISTS idx_pipeline_logs_stage_name
                    ON pipeline_logs(stage_name);
                """
            )
            self._migrate_db(conn)
            self._init_fts(conn)
            conn.commit()
        finally:
            conn.close()

    def _init_fts(self, conn):
        """Create the optional FTS5 index and maintenance triggers."""
        try:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS speeches_fts
                USING fts5(
                    title,
                    full_text,
                    content='speeches',
                    content_rowid='id'
                )
                """
            )
            conn.executescript(
                """
                CREATE TRIGGER IF NOT EXISTS speeches_ai
                AFTER INSERT ON speeches BEGIN
                    INSERT INTO speeches_fts(rowid, title, full_text)
                    VALUES (new.id, new.title, new.full_text);
                END;

                CREATE TRIGGER IF NOT EXISTS speeches_ad
                AFTER DELETE ON speeches BEGIN
                    INSERT INTO speeches_fts(
                        speeches_fts,
                        rowid,
                        title,
                        full_text
                    )
                    VALUES ('delete', old.id, old.title, old.full_text);
                END;

                CREATE TRIGGER IF NOT EXISTS speeches_au
                AFTER UPDATE ON speeches BEGIN
                    INSERT INTO speeches_fts(
                        speeches_fts,
                        rowid,
                        title,
                        full_text
                    )
                    VALUES ('delete', old.id, old.title, old.full_text);
                    INSERT INTO speeches_fts(rowid, title, full_text)
                    VALUES (new.id, new.title, new.full_text);
                END;
                """
            )
        except sqlite3.OperationalError:
            pass

    @staticmethod
    def _column_names(conn, table_name):
        rows = conn.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()
        return {row["name"] for row in rows}

    def _migrate_db(self, conn):
        """Apply migrations without dropping historical data."""
        member_columns = self._column_names(conn, "members")
        member_additions = [
            ("term_start", "TEXT"),
            ("term_end", "TEXT"),
            ("last_speech_date", "TEXT"),
            ("last_verified_at", "TEXT"),
            ("last_updated", "TEXT"),
            ("avg_stance_score", "REAL"),
        ]
        for column_name, column_type in member_additions:
            if column_name not in member_columns:
                conn.execute(
                    f"ALTER TABLE members ADD COLUMN "
                    f"{column_name} {column_type}"
                )

        speech_columns = self._column_names(conn, "speeches")
        if "updated_at" not in speech_columns:
            conn.execute("ALTER TABLE speeches ADD COLUMN updated_at TEXT")
        conn.execute(
            """
            UPDATE speeches
            SET updated_at = COALESCE(updated_at, fetched_at, created_at)
            WHERE updated_at IS NULL
            """
        )

        if "analysis_status" in speech_columns:
            self._split_legacy_analysis_columns(conn, speech_columns)

        analysis_columns = self._column_names(conn, "analysis_results")
        if "model_name" not in analysis_columns:
            conn.execute(
                "ALTER TABLE analysis_results ADD COLUMN model_name TEXT"
            )
        if "analysis_version" not in analysis_columns:
            conn.execute(
                """
                ALTER TABLE analysis_results
                ADD COLUMN analysis_version TEXT
                """
            )

        conn.execute(
            """
            UPDATE analysis_results
            SET analysis_status = 'failed'
            WHERE analysis_status = 'pending'
              AND COALESCE(analysis_attempts, 0) >= 3
            """
        )

        pipeline_columns = self._column_names(conn, "pipeline_logs")
        if pipeline_columns and "duration_seconds" not in pipeline_columns:
            conn.execute(
                """
                ALTER TABLE pipeline_logs
                ADD COLUMN duration_seconds REAL
                """
            )

    def _split_legacy_analysis_columns(self, conn, speech_columns):
        """Move legacy analysis fields out of speeches when necessary."""
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO analysis_results (
                    speech_id,
                    stance_score,
                    stance_reason,
                    keywords,
                    main_risk,
                    analysis_attempts,
                    analysis_status
                )
                SELECT
                    id,
                    stance_score,
                    stance_reason,
                    keywords,
                    main_risk,
                    analysis_attempts,
                    analysis_status
                FROM speeches
                WHERE analysis_status IS NOT NULL
                """
            )

            columns_to_remove = [
                "stance_score",
                "stance_reason",
                "keywords",
                "main_risk",
                "analysis_attempts",
                "analysis_status",
            ]
            conn.execute("DROP INDEX IF EXISTS idx_speeches_status")
            for column_name in columns_to_remove:
                if column_name in speech_columns:
                    conn.execute(
                        f"ALTER TABLE speeches DROP COLUMN {column_name}"
                    )
        except sqlite3.OperationalError:
            pass

    def backup_daily(self):
        """Create one consistent SQLite snapshot per UTC date."""
        return backup_sqlite_database(self.db_path)

    def log_pipeline_step(
        self,
        run_id,
        stage_name,
        started_at,
        finished_at,
        status,
        item_count=0,
        error_msg=None,
        details=None,
    ):
        """Record a single pipeline stage."""
        duration_seconds = None
        try:
            if started_at and finished_at:
                duration_seconds = (
                    datetime.fromisoformat(finished_at)
                    - datetime.fromisoformat(started_at)
                ).total_seconds()
        except (TypeError, ValueError):
            duration_seconds = None

        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO pipeline_logs (
                    run_id,
                    stage_name,
                    started_at,
                    finished_at,
                    duration_seconds,
                    status,
                    item_count,
                    error_message,
                    details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    stage_name,
                    started_at,
                    finished_at,
                    duration_seconds,
                    status,
                    item_count,
                    error_msg,
                    json.dumps(details) if details is not None else None,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_or_create_member(
        self,
        bank_code,
        name,
        role=None,
        status="active",
    ):
        """Return an existing member ID or create a member."""
        if not name:
            return None

        conn = self._get_conn()
        try:
            row = conn.execute(
                """
                SELECT id, role, status
                FROM members
                WHERE bank_code = ? AND name = ?
                """,
                (bank_code, name),
            ).fetchone()
            if row:
                if (role and row["role"] != role) or status != row["status"]:
                    conn.execute(
                        """
                        UPDATE members
                        SET role = COALESCE(?, role),
                            status = ?,
                            last_updated = ?
                        WHERE id = ?
                        """,
                        (role, status, utc_now_iso(), row["id"]),
                    )
                    conn.commit()
                return row["id"]

            cursor = conn.execute(
                """
                INSERT INTO members (
                    bank_code,
                    name,
                    role,
                    status,
                    last_updated
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (bank_code, name, role, status, utc_now_iso()),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_member_official(self, bank_code, name, **kwargs):
        """Update member details from an official roster."""
        now = datetime.now(timezone.utc)
        kwargs["last_verified_at"] = now.date().isoformat()
        kwargs["last_updated"] = now.isoformat()
        kwargs["status"] = "active"

        conn = self._get_conn()
        try:
            columns = [f"{key} = ?" for key in kwargs]
            values = list(kwargs.values())
            sql = (
                f"UPDATE members SET {', '.join(columns)} "
                "WHERE bank_code = ? AND name = ?"
            )
            cursor = conn.execute(sql, values + [bank_code, name])
            if cursor.rowcount == 0:
                insert_columns = ["bank_code", "name", *kwargs.keys()]
                placeholders = ", ".join(["?"] * len(insert_columns))
                conn.execute(
                    f"INSERT INTO members "
                    f"({', '.join(insert_columns)}) "
                    f"VALUES ({placeholders})",
                    [bank_code, name, *kwargs.values()],
                )
            conn.commit()
        finally:
            conn.close()

    def mark_missing_members_retired(
        self,
        bank_code,
        current_member_names,
    ):
        """Retire members missing from a verified official roster."""
        if not current_member_names:
            return 0

        placeholders = ", ".join(["?"] * len(current_member_names))
        sql = f"""
            UPDATE members
            SET status = 'retired',
                term_end = COALESCE(term_end, date('now')),
                last_updated = ?
            WHERE bank_code = ?
              AND status = 'active'
              AND name NOT IN ({placeholders})
        """
        params = [utc_now_iso(), bank_code, *current_member_names]

        conn = self._get_conn()
        try:
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def insert_speech(
        self,
        bank_code,
        speaker,
        title,
        date,
        url,
        full_text=None,
        speech_type="speech",
        language="en",
    ):
        """Insert a speech and update the speaker's last speech date."""
        speaker_id = self.get_or_create_member(bank_code, speaker)
        now = utc_now_iso()

        conn = self._get_conn()
        try:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO speeches (
                    bank_code,
                    speaker_id,
                    title,
                    date,
                    url,
                    full_text,
                    speech_type,
                    language,
                    fetched_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_code,
                    speaker_id,
                    title,
                    date,
                    url,
                    full_text,
                    speech_type,
                    language,
                    now,
                    now,
                    now,
                ),
            )
            if cursor.rowcount > 0 and speaker_id:
                conn.execute(
                    """
                    UPDATE members
                    SET last_speech_date = MAX(
                            COALESCE(last_speech_date, ''),
                            ?
                        ),
                        last_updated = ?
                    WHERE id = ?
                    """,
                    (date, now, speaker_id),
                )
            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        finally:
            conn.close()

    def get_incomplete_speeches(self, bank_code=None, days_limit=30):
        """Return recent speeches whose full text is still incomplete."""
        query = """
            SELECT id, url, title, date, fetched_at
            FROM speeches
            WHERE (
                full_text IS NULL
                OR length(full_text) < 500
                OR full_text LIKE '%to be published%'
            )
              AND date <= date('now')
              AND date >= date('now', ?)
        """
        params = [f"-{days_limit} days"]
        if bank_code:
            query += " AND bank_code = ?"
            params.append(bank_code)

        conn = self._get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def update_speech_content(self, speech_id, full_text, exact_date=None):
        """Update speech text and invalidate analysis when source data changes."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT full_text, date FROM speeches WHERE id = ?",
                (speech_id,),
            ).fetchone()
            if row is None:
                return False

            new_date = exact_date or row["date"]
            if row["full_text"] == full_text and row["date"] == new_date:
                return False

            now = utc_now_iso()
            conn.execute(
                """
                UPDATE speeches
                SET full_text = ?, date = ?, updated_at = ?
                WHERE id = ?
                """,
                (full_text, new_date, now, speech_id),
            )
            conn.execute(
                """
                UPDATE analysis_results
                SET stance_score = NULL,
                    stance_reason = NULL,
                    keywords = NULL,
                    main_risk = NULL,
                    analysis_attempts = 0,
                    analysis_status = 'pending',
                    analyzed_at = NULL,
                    model_name = NULL,
                    analysis_version = NULL
                WHERE speech_id = ?
                """,
                (speech_id,),
            )
            conn.commit()
            return True
        finally:
            conn.close()

    def count_speeches_for_year(self, bank_code, year):
        """Return stored speech count for one bank and calendar year."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM speeches
                WHERE bank_code = ?
                  AND substr(date, 1, 4) = ?
                """,
                (bank_code, str(year)),
            ).fetchone()
            return int(row["count"])
        finally:
            conn.close()

    def get_existing_urls(self, bank_code):
        """Return stored speech URLs for one central bank."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT url FROM speeches WHERE bank_code = ?",
                (bank_code,),
            ).fetchall()
            return {row["url"] for row in rows}
        finally:
            conn.close()

    def search_speeches(self, keyword):
        """Search titles and full text through SQLite FTS5."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT s.bank_code, s.date, m.name AS speaker, s.title
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                JOIN speeches_fts f ON s.id = f.rowid
                WHERE speeches_fts MATCH ?
                ORDER BY rank
                """,
                (keyword,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_stats(self):
        """Return speech and completed-analysis counts by bank."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    s.bank_code,
                    COUNT(s.id) AS total,
                    SUM(
                        CASE
                            WHEN ar.analysis_status IN ('scored', 'no_signal')
                            THEN 1
                            ELSE 0
                        END
                    ) AS analyzed
                FROM speeches s
                LEFT JOIN analysis_results ar ON s.id = ar.speech_id
                GROUP BY s.bank_code
                """
            ).fetchall()
            stats = {
                row["bank_code"]: {
                    "total_speeches": row["total"],
                    "analyzed": row["analyzed"],
                }
                for row in rows
            }
            total = conn.execute(
                "SELECT COUNT(*) AS count FROM speeches"
            ).fetchone()
            stats["total"] = total["count"]
            return stats
        finally:
            conn.close()

    def update_all_member_stances(self):
        """Update weighted historical stance scores for all speakers."""
        conn = self._get_conn()
        try:
            members = conn.execute(
                """
                SELECT DISTINCT m.id
                FROM members m
                JOIN speeches s ON m.id = s.speaker_id
                JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE ar.stance_score IS NOT NULL
                """
            ).fetchall()

            today = datetime.now(timezone.utc).date()
            now = utc_now_iso()
            for member in members:
                speeches = conn.execute(
                    """
                    SELECT ar.stance_score, s.date
                    FROM speeches s
                    JOIN analysis_results ar ON s.id = ar.speech_id
                    WHERE s.speaker_id = ?
                      AND ar.stance_score IS NOT NULL
                    ORDER BY s.date DESC
                    """,
                    (member["id"],),
                ).fetchall()

                total_weight = 0.0
                total_score = 0.0
                for speech in speeches:
                    try:
                        speech_date = datetime.strptime(
                            speech["date"][:10],
                            "%Y-%m-%d",
                        ).date()
                    except (TypeError, ValueError):
                        continue

                    age_days = (today - speech_date).days
                    if age_days <= 90:
                        weight = 1.0
                    elif age_days <= 180:
                        weight = 0.5
                    elif age_days <= 365:
                        weight = 0.2
                    else:
                        weight = 0.05

                    total_weight += weight
                    total_score += speech["stance_score"] * weight

                if total_weight > 0:
                    conn.execute(
                        """
                        UPDATE members
                        SET avg_stance_score = ?, last_updated = ?
                        WHERE id = ?
                        """,
                        (
                            total_score / total_weight,
                            now,
                            member["id"],
                        ),
                    )
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    database = SpeechDB()
    print(
        "Database initialized. Total speeches: "
        f"{database.get_stats().get('total', 0)}"
    )
