import sqlite3

from tools.speech_tracker.analyzer import (
    HawkDoveAnalyzer,
    KeywordItem,
    MAX_ANALYSIS_ATTEMPTS,
    StanceResult,
)


class TempDB:
    def __init__(self, db_path):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


def test_stance_result_accepts_valid_score():
    data = {
        "stance_score": 0.5,
        "stance_reason": "Test reason.",
        "keywords": [
            {"category": "Inflation", "detail": "Test"}
        ],
        "main_risk": "Test Risk",
    }

    model = StanceResult(**data)

    assert model.stance_score == 0.5
    assert len(model.keywords) == 1
    assert model.keywords[0] == KeywordItem(
        category="Inflation",
        detail="Test",
    )


def test_stance_result_accepts_null_score():
    data = {
        "stance_score": None,
        "stance_reason": "No policy signal.",
        "keywords": [],
        "main_risk": None,
    }

    model = StanceResult(**data)

    assert model.stance_score is None
    assert model.main_risk is None


def _create_analysis_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE speeches (
            id INTEGER PRIMARY KEY,
            full_text TEXT
        );
        CREATE TABLE analysis_results (
            speech_id INTEGER PRIMARY KEY,
            stance_score REAL,
            stance_reason TEXT,
            keywords TEXT,
            main_risk TEXT,
            analysis_attempts INTEGER,
            analysis_status TEXT,
            analyzed_at TEXT,
            model_name TEXT,
            analysis_version TEXT
        );
        """
    )
    conn.commit()
    conn.close()


def test_revive_skipped_speeches_with_text(tmp_path):
    db_path = tmp_path / "speeches.db"
    _create_analysis_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        INSERT INTO speeches (id, full_text) VALUES
            (1, 'short'),
            (2, printf('%.*c', 501, 'x'));
        INSERT INTO analysis_results (
            speech_id,
            stance_reason,
            keywords,
            analysis_attempts,
            analysis_status,
            analyzed_at,
            model_name,
            analysis_version
        ) VALUES
            (1, 'Skipped: Text too short.', '[]', 1, 'skipped',
             '2026-01-01T00:00:00+00:00', NULL, NULL),
            (2, 'Skipped: Text too short.', '[]', 1, 'skipped',
             '2026-01-01T00:00:00+00:00', NULL, NULL);
        """
    )
    conn.commit()
    conn.close()

    analyzer = HawkDoveAnalyzer.__new__(HawkDoveAnalyzer)
    analyzer.db = TempDB(db_path)

    assert analyzer.revive_skipped_speeches_with_text() == 1

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT speech_id, analysis_status, analysis_attempts,
               stance_reason, keywords, analyzed_at,
               model_name, analysis_version
        FROM analysis_results
        ORDER BY speech_id
        """
    ).fetchall()
    conn.close()

    assert rows[0] == (
        1,
        "skipped",
        1,
        "Skipped: Text too short.",
        "[]",
        "2026-01-01T00:00:00+00:00",
        None,
        None,
    )
    assert rows[1] == (
        2,
        "pending",
        0,
        None,
        None,
        None,
        None,
        None,
    )


def test_exhausted_pending_analysis_becomes_failed(tmp_path):
    db_path = tmp_path / "speeches.db"
    _create_analysis_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO analysis_results (
            speech_id,
            analysis_attempts,
            analysis_status
        ) VALUES (1, ?, 'pending')
        """,
        (MAX_ANALYSIS_ATTEMPTS,),
    )
    conn.commit()
    conn.close()

    analyzer = HawkDoveAnalyzer.__new__(HawkDoveAnalyzer)
    analyzer.db = TempDB(db_path)

    assert analyzer.mark_exhausted_analysis_as_failed() == 1

    conn = sqlite3.connect(db_path)
    status = conn.execute(
        "SELECT analysis_status FROM analysis_results WHERE speech_id = 1"
    ).fetchone()[0]
    conn.close()

    assert status == "failed"
