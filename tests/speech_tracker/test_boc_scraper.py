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

    speeches = scraper._parse_speech_list_page(scraper._parse_html(html))

    assert len(speeches) == 1
    assert speeches[0]["title"] == "An anchor of stability in uncertain times"
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
    assert cleaned.endswith("Our goal is to be an anchor of stability in uncertain times.")
