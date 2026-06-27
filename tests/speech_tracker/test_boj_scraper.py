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


def test_boj_fetch_speech_list_parses_current_table(monkeypatch):
    html = """
    <table>
      <tr>
        <th>Date</th><th>Speaker</th><th>Title</th>
      </tr>
      <tr>
        <td>May&nbsp;&nbsp;21,&nbsp;2026</td>
        <td>KOEDA Junko, Member of the Policy Board</td>
        <td><a href="/en/about/press/koen_2026/ko260521a.htm">
          "Economic Activity, Prices, and Monetary Policy in Japan"
        </a></td>
      </tr>
    </table>
    """

    class Response:
        text = html

    def fake_get(self, url, **kwargs):
        return Response()

    monkeypatch.setattr(BOJScraper, "_get", fake_get)

    scraper = BOJScraper(db=object())
    speeches = scraper.fetch_speech_list(year=2026)

    assert speeches == [
        {
            "title": '"Economic Activity, Prices, and Monetary Policy in Japan"',
            "date": "2026-05-21",
            "url": "https://www.boj.or.jp/en/about/press/koen_2026/ko260521a.htm",
            "speaker": "Junko Koeda",
        }
    ]
