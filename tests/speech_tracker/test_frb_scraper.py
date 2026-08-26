from tools.speech_tracker.scrapers.frb import FRBScraper


class Response:
    def __init__(self, text):
        self.text = text


def test_frb_prefers_full_speaker_name_from_list_context(monkeypatch):
    html = """
    <div class="row eventlist">
      <h4>
        <a href="/newsevents/speech/cook20260805a.htm">
          Outlook for the U.S. and Alaskan Economies
        </a>
      </h4>
      <p>Governor Lisa D. Cook</p>
    </div>
    """
    scraper = FRBScraper(db=object())
    monkeypatch.setattr(scraper, "_get", lambda url: Response(html))

    speeches = scraper.fetch_speech_list(year=2026)

    assert len(speeches) == 1
    assert speeches[0]["speaker"] == "Lisa D. Cook"
    assert speeches[0]["date"] == "2026-08-05"
