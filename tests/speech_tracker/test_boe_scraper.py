from tools.speech_tracker.scrapers.boe import BOEScraper


def test_boe_keeps_distinct_urls_with_same_title_and_date(monkeypatch):
    html = """
    <main>
      <a href="/news/speeches/2026/july/opening-a">
        <h3 class="list">Opening Remarks</h3>
        <time class="release-date" datetime="2026-07-01"></time>
        <div class="release-tag">Speech // Speaker A</div>
      </a>
      <a href="/news/speeches/2026/july/opening-b">
        <h3 class="list">Opening Remarks</h3>
        <time class="release-date" datetime="2026-07-01"></time>
        <div class="release-tag">Speech // Speaker B</div>
      </a>
    </main>
    """
    scraper = BOEScraper(db=object())
    monkeypatch.setattr(scraper, "_get_playwright", lambda url: html)

    speeches = scraper.fetch_speech_list(year=2026)

    assert len(speeches) == 2
    assert {speech["speaker"] for speech in speeches} == {
        "Speaker A",
        "Speaker B",
    }


def test_boe_full_collection_walks_requested_years(monkeypatch):
    scraper = BOEScraper(db=object())
    captured_years = []

    def fake_fetch(year=None):
        captured_years.append(year)
        return []

    monkeypatch.setattr(scraper, "fetch_speech_list", fake_fetch)

    assert scraper.get_all_speeches(
        start_year=2025,
        end_year=2026,
    ) == []
    assert captured_years == [2026, 2025]
