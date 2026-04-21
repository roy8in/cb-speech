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
        raise AssertionError("CSV path should not be used for recent ECB collection")

    monkeypatch.setattr(scraper, "_fetch_from_csv", fail_if_called)
    monkeypatch.setattr(
        scraper,
        "fetch_recent_from_html",
        lambda: [
            {
                "title": "Recent ECB Speech",
                "date": "2026-04-21",
                "url": "https://www.ecb.europa.eu/press/key/recent-speech",
                "speaker": "Christine Lagarde",
            }
        ],
    )
    monkeypatch.setattr(scraper, "fetch_speech_text", lambda url: "Recent ECB Speech full text")

    count = scraper.collect_recent(fetch_text=True)

    assert count == 1
    assert db.inserted["url"] == "https://www.ecb.europa.eu/press/key/recent-speech"
    assert db.inserted["full_text"] == "Recent ECB Speech full text"
