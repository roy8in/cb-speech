import sqlite3

from tools.speech_tracker.models import SpeechDB


def _create_legacy_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_code TEXT NOT NULL,
            name TEXT NOT NULL,
            role TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(bank_code, name)
        );
        CREATE TABLE speeches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_code TEXT NOT NULL,
            speaker_id INTEGER,
            title TEXT NOT NULL,
            date TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            full_text TEXT,
            speech_type TEXT,
            language TEXT,
            fetched_at TEXT NOT NULL,
            created_at TEXT
        );
        CREATE TABLE analysis_results (
            speech_id INTEGER PRIMARY KEY,
            stance_score REAL,
            stance_reason TEXT,
            keywords TEXT,
            main_risk TEXT,
            analysis_attempts INTEGER,
            analysis_status TEXT,
            analyzed_at TEXT
        );
        CREATE TABLE pipeline_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT,
            item_count INTEGER,
            error_message TEXT,
            details_json TEXT
        );
        INSERT INTO speeches (
            bank_code,
            title,
            date,
            url,
            full_text,
            fetched_at,
            created_at
        ) VALUES (
            'FRB',
            'Legacy speech',
            '2026-08-01',
            'https://example.com/legacy',
            'old text',
            '2026-08-01T12:00:00+00:00',
            '2026-08-01T12:00:00+00:00'
        );
        """
    )
    conn.commit()
    conn.close()


def test_existing_db_is_backed_up_before_migration(tmp_path):
    db_path = tmp_path / "speeches.db"
    _create_legacy_db(db_path)

    db = SpeechDB(db_path)

    assert db.backup_path is not None
    assert db.backup_path.exists()

    backup_conn = sqlite3.connect(db.backup_path)
    backup_columns = {
        row[1]
        for row in backup_conn.execute("PRAGMA table_info(speeches)")
    }
    backup_conn.close()

    live_conn = sqlite3.connect(db_path)
    live_columns = {
        row[1]
        for row in live_conn.execute("PRAGMA table_info(speeches)")
    }
    live_conn.close()

    assert "updated_at" not in backup_columns
    assert "updated_at" in live_columns


def test_refresh_invalidates_existing_analysis(tmp_path):
    db = SpeechDB(tmp_path / "speeches.db")
    speech_id = db.insert_speech(
        bank_code="FRB",
        speaker="Test Speaker",
        title="Test Speech",
        date="2026-08-01",
        url="https://example.com/speech",
        full_text="old text",
    )

    conn = db._get_conn()
    conn.execute(
        """
        INSERT INTO analysis_results (
            speech_id,
            stance_score,
            stance_reason,
            keywords,
            main_risk,
            analysis_attempts,
            analysis_status,
            analyzed_at,
            model_name,
            analysis_version
        ) VALUES (?, 0.5, 'old', '[]', 'risk', 1, 'scored',
                  '2026-08-01T13:00:00+00:00',
                  'gemini-2.5-flash', 'hawk_dove_v1')
        """,
        (speech_id,),
    )
    conn.commit()
    conn.close()

    changed = db.update_speech_content(
        speech_id,
        "new recovered text",
        exact_date="2026-08-02",
    )

    conn = db._get_conn()
    row = conn.execute(
        """
        SELECT analysis_status, analysis_attempts, stance_score,
               analyzed_at, model_name, analysis_version
        FROM analysis_results
        WHERE speech_id = ?
        """,
        (speech_id,),
    ).fetchone()
    conn.close()

    assert changed is True
    assert row["analysis_status"] == "pending"
    assert row["analysis_attempts"] == 0
    assert row["stance_score"] is None
    assert row["analyzed_at"] is None
    assert row["model_name"] is None
    assert row["analysis_version"] is None
