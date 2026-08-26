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

    def __init__(self, db, speech_list):
        self.speech_list = speech_list
        super().__init__(db=db)

    def fetch_speech_list(self, year=None):
        return list(self.speech_list)

    def fetch_speech_text(self, url):
        return "x" * 600


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
