from tools.speech_tracker.scrapers.rba import RBAScraper


class HeadResponse:
    headers = {}


def test_rba_detail_page_returns_speaker_marker(monkeypatch):
    html = """
    <main>
      <h1>The Road to Ample</h1>
      <p>David Jacobs</p>
      <p>Head of Domestic Markets</p>
      <p>Policy text begins here.</p>
    </main>
    """
    scraper = RBAScraper(db=object())
    monkeypatch.setattr(scraper.session, "head", lambda *args, **kwargs: HeadResponse())
    monkeypatch.setattr(scraper, "_get_playwright", lambda url: html)

    text = scraper.fetch_speech_text(
        "https://www.rba.gov.au/speeches/2026/sp-so-2026-08-25.html"
    )

    assert text.startswith("__SPEAKER__:David Jacobs\n")
