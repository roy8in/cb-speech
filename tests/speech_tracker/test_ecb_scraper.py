import pytest

from tools.speech_tracker.scrapers.ecb import ECBScraper


class MockDB:
    def get_existing_urls(self, bank_code):
        return set()

    def insert_speech(self, **kwargs):
        self.inserted = kwargs
        return 1


def test_ecb_collect_recent_uses_rss_only(monkeypatch):
    db = MockDB()
    scraper = ECBScraper(db=db)

    def fail_if_called(*args, **kwargs):
        raise AssertionError(
            "CSV path should not be used for recent ECB collection"
        )

    monkeypatch.setattr(scraper, "_fetch_from_csv", fail_if_called)
    monkeypatch.setattr(
        scraper,
        "fetch_recent_speeches",
        lambda: [
            {
                "title": "Recent ECB Speech",
                "date": "2026-04-21",
                "url": (
                    "https://www.ecb.europa.eu/press/key/recent-speech"
                ),
                "speaker": "Christine Lagarde",
            }
        ],
    )
    monkeypatch.setattr(
        scraper,
        "fetch_speech_text",
        lambda url: "Recent ECB Speech full text",
    )

    count = scraper.collect_recent(fetch_text=True)

    assert count == 1
    assert db.inserted["url"] == (
        "https://www.ecb.europa.eu/press/key/recent-speech"
    )
    assert db.inserted["full_text"] == "Recent ECB Speech full text"


def test_ecb_full_list_uses_csv_only(monkeypatch):
    scraper = ECBScraper(db=MockDB())
    expected = [
        {
            "title": "Dataset Speech",
            "date": "2026-04-20",
            "url": "ecb://speeches/2026-04-20/dataset-speech",
            "speaker": "Christine Lagarde",
            "_full_text": "Text",
        }
    ]

    monkeypatch.setattr(
        scraper,
        "_fetch_from_csv",
        lambda year=None: expected,
    )

    assert scraper.fetch_speech_list(year=2026) == expected


def test_ecb_recent_source_failure_is_not_silent(monkeypatch):
    scraper = ECBScraper(db=MockDB())
    monkeypatch.setattr(scraper, "fetch_recent_speeches", lambda: None)

    with pytest.raises(
        RuntimeError,
        match="could not be fetched or parsed",
    ):
        scraper.collect_recent(fetch_text=False)


def test_ecb_extracts_current_rss_speaker_prefix():
    assert ECBScraper._extract_rss_speaker(
        "Christine Lagarde: Panel remarks about the European economy"
    ) == "Christine Lagarde"
    assert ECBScraper._extract_rss_speaker(
        "Christine Lagarde, Boris Vujcic: Monetary policy statement"
    ) == "Christine Lagarde, Boris Vujcic"
