import pytest
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.speech_tracker.analyzer import HawkDoveAnalyzer

class MockDB:
    def _get_conn(self):
        return MagicMock()

@pytest.fixture
def analyzer():
    db = MockDB()
    # Mocking check_api_status so we can test non-LLM dependent methods if needed
    a = HawkDoveAnalyzer(db)
    return a

def test_json_parsing_resilience():
    # Since analyze_text relies entirely on LLM now via Pydantic schema,
    # the main resilience test is ensuring schema fields are respected.
    from tools.speech_tracker.analyzer import StanceResult, KeywordItem
    
    # Simulate a valid valid pydantic output
    valid_data = {
        "stance_score": 0.5,
        "stance_reason": "Test reason.",
        "keywords": [{"category": "Inflation", "detail": "Test"}],
        "main_risk": "Test Risk"
    }
    
    model = StanceResult(**valid_data)
    assert model.stance_score == 0.5
    assert len(model.keywords) == 1
    assert model.keywords[0].category == "Inflation"

def test_json_parsing_null_score():
    from tools.speech_tracker.analyzer import StanceResult
    
    valid_data = {
        "stance_score": None,
        "stance_reason": "No policy signal.",
        "keywords": [],
        "main_risk": None
    }
    
    model = StanceResult(**valid_data)
    assert model.stance_score is None
    assert model.main_risk is None


class TempDB:
    def __init__(self, db_path):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


def test_revive_skipped_speeches_with_text(tmp_path):
    db_path = tmp_path / "speeches.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
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
            synced_at TEXT
        );
        INSERT INTO speeches (id, full_text) VALUES
            (1, 'short'),
            (2, printf('%.*c', 501, 'x'));
        INSERT INTO analysis_results
            (speech_id, stance_score, stance_reason, keywords, main_risk, analysis_attempts, analysis_status, analyzed_at, synced_at)
        VALUES
            (1, NULL, 'Skipped: Text too short.', '[]', NULL, 1, 'skipped', '2026-01-01', NULL),
            (2, NULL, 'Skipped: Text too short.', '[]', NULL, 1, 'skipped', '2026-01-01', '2026-01-02');
    """)
    conn.commit()
    conn.close()

    analyzer = HawkDoveAnalyzer.__new__(HawkDoveAnalyzer)
    analyzer.db = TempDB(db_path)

    assert analyzer.revive_skipped_speeches_with_text() == 1

    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT speech_id, analysis_status, analysis_attempts, stance_reason, keywords, analyzed_at, synced_at
        FROM analysis_results
        ORDER BY speech_id
    """).fetchall()
    conn.close()

    assert rows[0] == (1, "skipped", 1, "Skipped: Text too short.", "[]", "2026-01-01", None)
    assert rows[1] == (2, "pending", 0, None, None, None, None)
