from datetime import datetime

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.scrapers.boc import BOCScraper


def test_boc_list_parser_uses_main_results_and_content_types():
    html = """
    <html>
      <body>
        <div class="media">
          <span class="media-date">April 20, 2026</span>
          <h3><a href="https://www.bankofcanada.ca/2026/04/news-item/">Bank of Canada announces the appointment of two Deputy Governors</a></h3>
        </div>
        <main>
          <div class="media">
            <span class="media-date">March 26, 2026</span>
            <h3><a href="https://www.bankofcanada.ca/multimedia/speech-brandon-chamber-commerce/">Speech: Brandon Chamber of Commerce</a></h3>
            <p>Content Type(s): <a>Press</a>, <a>Speeches and appearances</a>, <a>Webcasts</a></p>
          </div>
          <div class="media">
            <span class="media-date">March 26, 2026</span>
            <h3><a href="https://www.bankofcanada.ca/2026/03/an-anchor-of-stability-in-uncertain-times/">An anchor of stability in uncertain times</a></h3>
            <p>Remarks <a href="/profile/carolyn-rogers/">Carolyn Rogers</a> Brandon Chamber of Commerce</p>
            <p>Content Type(s): <a>Press</a>, <a>Speeches and appearances</a>, <a>Remarks</a></p>
          </div>
        </main>
      </body>
    </html>
    """
    scraper = BOCScraper(db=object())

    speeches = scraper._parse_speech_list_page(
        scraper._parse_html(html)
    )

    assert len(speeches) == 1
    assert speeches[0]["title"] == (
        "An anchor of stability in uncertain times"
    )
    assert speeches[0]["speaker"] == "Carolyn Rogers"
    assert speeches[0]["speech_type"] == "Remarks"


def test_boc_clean_speech_text_removes_page_chrome_and_related_items():
    scraper = BOCScraper(db=object())
    text = """
    An anchor of stability in uncertain times
    Remarks
    Carolyn Rogers
    Share this page on Facebook
    Share this page on Facebook
    Available as:
    PDF
    Introduction
    Good afternoon.
    Conclusion
    Our goal is to be an anchor of stability in uncertain times.
    Content Type(s)
    :
    Press
    Related Information
    Speech: Brandon Chamber of Commerce
    """

    cleaned = scraper._clean_speech_text(text)

    assert "Share this page" not in cleaned
    assert "Available as:" not in cleaned
    assert "Content Type(s)" not in cleaned
    assert "Related Information" not in cleaned
    assert cleaned.endswith(
        "Our goal is to be an anchor of stability in uncertain times."
    )


def test_boc_collect_recent_accepts_new_url(tmp_path, monkeypatch):
    current_year = datetime.now().year
    db = SpeechDB(tmp_path / "speeches.db")
    scraper = BOCScraper(db=db)
    speech = {
        "title": "Opening Statement",
        "date": f"{current_year}-08-01",
        "url": (
            "https://www.bankofcanada.ca/"
            f"{current_year}/08/opening-statement/"
        ),
        "speaker": "Tiff Macklem",
        "speech_type": "Opening statements",
    }

    monkeypatch.setattr(
        scraper,
        "fetch_speech_list",
        lambda year=None: [speech.copy()],
    )
    monkeypatch.setattr(
        scraper,
        "fetch_speech_text",
        lambda url: "Policy text. " * 60,
    )

    assert scraper.collect_recent(fetch_text=True) == 1
    assert db.get_stats()["BOC"]["total_speeches"] == 1


def test_boc_refresh_uses_current_schema_and_invalidates_analysis(
    tmp_path,
    monkeypatch,
):
    current_year = datetime.now().year
    db = SpeechDB(tmp_path / "speeches.db")
    url = (
        "https://www.bankofcanada.ca/"
        f"{current_year}/08/policy-remarks/"
    )
    speech_id = db.insert_speech(
        bank_code="BOC",
        speaker="Tiff Macklem",
        title="Policy Remarks",
        date=f"{current_year}-08-01",
        url=url,
        full_text=("Share this page\n" + ("Old text. " * 80)),
        speech_type="Remarks",
    )

    conn = db._get_conn()
    try:
        conn.execute(
            """
            INSERT INTO analysis_results (
                speech_id,
                stance_score,
                stance_reason,
                analysis_attempts,
                analysis_status
            )
            VALUES (?, 0.5, 'Old result', 1, 'scored')
            """,
            (speech_id,),
        )
        conn.commit()
    finally:
        conn.close()

    scraper = BOCScraper(db=db)
    speech = {
        "title": "Policy Remarks",
        "date": f"{current_year}-08-01",
        "url": url,
        "speaker": "Tiff Macklem",
        "speech_type": "Remarks",
    }
    monkeypatch.setattr(
        scraper,
        "fetch_speech_list",
        lambda year=None: [speech.copy()],
    )
    monkeypatch.setattr(
        scraper,
        "fetch_speech_text",
        lambda requested_url: "Clean policy text. " * 60,
    )

    assert scraper.collect_recent(fetch_text=True) == 0

    conn = db._get_conn()
    try:
        result = conn.execute(
            """
            SELECT analysis_status, stance_score, analysis_attempts
            FROM analysis_results
            WHERE speech_id = ?
            """,
            (speech_id,),
        ).fetchone()
    finally:
        conn.close()

    assert result["analysis_status"] == "pending"
    assert result["stance_score"] is None
    assert result["analysis_attempts"] == 0
