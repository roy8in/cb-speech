from types import SimpleNamespace

from tools.speech_tracker.scrapers import boj as boj_module
from tools.speech_tracker.scrapers.boj import BOJScraper


class DummyResponse:
    text = ""


def test_boj_fetch_speech_list_defaults_to_current_year(monkeypatch):
    captured = {}

    def fake_get(self, url, **kwargs):
        captured["url"] = url
        return DummyResponse()

    class FakeDatetime:
        @classmethod
        def now(cls):
            return SimpleNamespace(year=2030)

    monkeypatch.setattr(boj_module, "datetime", FakeDatetime)
    monkeypatch.setattr(BOJScraper, "_get", fake_get)

    scraper = BOJScraper(db=object())
    speeches = scraper.fetch_speech_list()

    assert "koen_2030" in captured["url"]
    assert speeches == []
