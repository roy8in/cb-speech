import pytest
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
