from datetime import datetime

import pytest

from tools.speech_tracker.scrapers.base import BaseScraper


class FakeDB:
    def __init__(self, existing_year_count=0):
        self.existing_year_count = existing_year_count
        self.inserted = []

    def get_existing_urls(self, bank_code):
        return set()

    def count_speeches_for_year(self, bank_code, year):
        return self.existing_year_count

    def insert_speech(self, **kwargs):
        self.inserted.append(kwargs)
        return len(self.inserted)


class FakeScraper(BaseScraper):
    BANK_CODE = "TEST"
    BANK_NAME = "Test Bank"
    BASE_URL = "https://example.com"

    def __init__(self, db, speech_list, fetched_text=None):
        self.speech_list = speech_list
        self.fetched_text = fetched_text or ("x" * 600)
        super().__init__(db=db)

    def fetch_speech_list(self, year=None):
        return list(self.speech_list)

    def fetch_speech_text(self, url):
        return self.fetched_text


def test_recent_zero_result_fails_for_populated_year():
    db = FakeDB(existing_year_count=3)
    scraper = FakeScraper(db, [])

    with pytest.raises(RuntimeError, match="returned 0 speeches"):
        scraper.collect_recent(fetch_text=False)


def test_same_title_and_date_with_different_urls_are_kept():
    current_year = datetime.now().year
    speeches = [
        {
            "title": "Opening Remarks",
            "date": f"{current_year}-08-01",
            "url": "https://example.com/a",
            "speaker": "Speaker A",
        },
        {
            "title": "Opening Remarks",
            "date": f"{current_year}-08-01",
            "url": "https://example.com/b",
            "speaker": "Speaker B",
        },
    ]
    db = FakeDB()
    scraper = FakeScraper(db, speeches)

    count = scraper.collect_recent(fetch_text=False)

    assert count == 2
    assert [row["url"] for row in db.inserted] == [
        "https://example.com/a",
        "https://example.com/b",
    ]


def test_recent_collection_applies_embedded_metadata():
    current_year = datetime.now().year
    speech = {
        "title": "Policy Speech",
        "date": f"{current_year}-08-01",
        "url": "https://example.com/policy-speech",
        "speaker": None,
    }
    fetched_text = (
        f"__DATE__:{current_year}-08-25\n"
        "__SPEAKER__:Actual Speaker\n"
        + ("Policy text. " * 60)
    )
    db = FakeDB()
    scraper = FakeScraper(db, [speech], fetched_text=fetched_text)

    count = scraper.collect_recent(fetch_text=True)

    assert count == 1
    inserted = db.inserted[0]
    assert inserted["date"] == f"{current_year}-08-25"
    assert inserted["speaker"] == "Actual Speaker"
    assert "__DATE__" not in inserted["full_text"]
    assert "__SPEAKER__" not in inserted["full_text"]
