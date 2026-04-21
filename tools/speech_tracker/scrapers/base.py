"""
Base scraper class for central bank speeches.
All 6 scrapers inherit from this.
"""

import requests
import logging
import time
import urllib3
import io
import atexit
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime

# Suppress SSL warnings for corporate proxy environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for central bank speech scrapers."""

    BANK_CODE = None  # Override in subclasses: 'FRB', 'ECB', etc.
    BANK_NAME = None  # Override: 'Federal Reserve', etc.
    BASE_URL = None   # Override: base URL of the speeches page

    # Polite scraping defaults
    REQUEST_DELAY = 0.5  # seconds between requests (lowered for historical backfill)
    REQUEST_TIMEOUT = 30
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

    def __init__(self, db=None):
        from tools.speech_tracker.models import SpeechDB
        self.db = db or SpeechDB()
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._playwright_manager = None
        self._playwright_browser = None
        self._playwright_context = None
        atexit.register(self.close)

    def _get(self, url, **kwargs):
        """Make a GET request with delay and error handling."""
        time.sleep(self.REQUEST_DELAY)
        try:
            # TLS verification is intentionally disabled for the current proxy/certificate environment.
            resp = self.session.get(url, timeout=self.REQUEST_TIMEOUT, verify=False, **kwargs)
            resp.raise_for_status()
            
            # Prevent requests from defaulting to ISO-8859-1 for text/html without charset
            if resp.encoding is None or resp.encoding.lower() == 'iso-8859-1':
                resp.encoding = resp.apparent_encoding or 'utf-8'
                
            return resp
        except requests.RequestException as e:
            logger.error(f"[{self.BANK_CODE}] Request failed for {url}: {e}")
            return None

    def _parse_html(self, html_text):
        """Parse HTML with BeautifulSoup."""
        return BeautifulSoup(html_text, 'html.parser')

    def extract_pdf_text(self, pdf_bytes):
        """Extract text from a PDF file using pdfplumber."""
        try:
            import pdfplumber
        except ImportError:
            logger.error("pdfplumber is not installed. Run: pip install pdfplumber")
            return "Error: pdfplumber not installed. Cannot extract PDF text."

        text_pages = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        # Clean up common PDF issues: replace multiple spaces with one, 
                        # but keep paragraph breaks (double newlines) if possible.
                        # For simplicity, we just join pages here.
                        text_pages.append(text)
                        
            full_text = "\n\n".join(text_pages).strip()
            
            # Basic cleanup: remove simple hyphenation at end of lines
            import re
            full_text = re.sub(r'(\w+)-\n([a-z]+)', r'\1\2', full_text)
            
            if not full_text or len(full_text) < 50:
                return "This speech appears to be a scanned document or contains no extractable text."
                
            return full_text
        except Exception as e:
            logger.error(f"[{self.BANK_CODE}] Failed to parse PDF: {e}")
            return "Error: Failed to extract text from this PDF document."

    def _get_playwright(self, url, wait_ms=2000):
        """Use Playwright to get page content, bypassing bot protection and dynamic loading."""
        from playwright.sync_api import sync_playwright
        import time
        try:
            if self._playwright_manager is None:
                self._playwright_manager = sync_playwright().start()
                self._playwright_browser = self._playwright_manager.chromium.launch(headless=True)
                self._playwright_context = self._playwright_browser.new_context(
                    user_agent=self.HEADERS['User-Agent']
                )

            page = self._playwright_context.new_page()
            try:
                page.goto(url, wait_until='networkidle', timeout=self.REQUEST_TIMEOUT * 1000)
                if wait_ms > 0:
                    time.sleep(wait_ms / 1000)
                content = page.content()
                return content
            finally:
                page.close()
        except Exception as e:
            logger.error(f"[{self.BANK_CODE}] Playwright failed for {url}: {e}")
            self._close_playwright()
            return None

    def _close_playwright(self):
        if self._playwright_context is not None:
            try:
                self._playwright_context.close()
            except Exception:
                pass
            self._playwright_context = None

        if self._playwright_browser is not None:
            try:
                self._playwright_browser.close()
            except Exception:
                pass
            self._playwright_browser = None

        if self._playwright_manager is not None:
            try:
                self._playwright_manager.stop()
            except Exception:
                pass
            self._playwright_manager = None

    def close(self):
        self._close_playwright()

    @abstractmethod
    def fetch_speech_list(self, year=None):
        """
        Fetch list of speeches from the central bank website.

        Returns list of dicts:
        [
            {
                'title': str,
                'date': str (YYYY-MM-DD),
                'url': str (full URL),
                'speaker': str or None,
            },
            ...
        ]
        """
        pass

    @abstractmethod
    def fetch_speech_text(self, url):
        """
        Fetch the full text of a speech from its URL.

        Returns: str (full text) or None
        """
        pass

    def refresh_incomplete_speeches(self):
        """Find and re-fetch speeches that were incomplete or placeholders."""
        incomplete = self.db.get_incomplete_speeches(self.BANK_CODE)
        if not incomplete:
            return 0
        
        refreshed_count = 0
        for item in incomplete:
            logger.info(f"[{self.BANK_CODE}] Refreshing: {item['title']} ({item['url']})")
            full_text = self.fetch_speech_text(item['url'])
            
            if full_text:
                exact_date = None
                if full_text.startswith("__DATE__:"):
                    parts = full_text.split("\n", 1)
                    exact_date = parts[0].replace("__DATE__:", "").strip()
                    full_text = parts[1].strip() if len(parts) > 1 else ""

                # Only update if we actually got real content now
                if len(full_text) > 500 and "to be published" not in full_text.lower():
                    self.db.update_speech_content(item['id'], full_text, exact_date)
                    refreshed_count += 1
                    logger.info(f"[{self.BANK_CODE}] Successfully refreshed ID {item['id']}")
            
        return refreshed_count

    def get_all_speeches(self, start_year=None, end_year=None):
        """
        Fetch ALL available speeches across all years.
        Override in subclasses if year-based pagination is needed.
        """
        current_year = datetime.now().year
        start = start_year or 2000
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):  # newest first
            try:
                speeches = self.fetch_speech_list(year=year)
                if speeches:
                    all_speeches.extend(speeches)
                    logger.info(f"[{self.BANK_CODE}] {year}: {len(speeches)} speeches found")
            except Exception as e:
                logger.warning(f"[{self.BANK_CODE}] Failed to fetch {year}: {e}")
                continue
        return all_speeches

    def normalize_url(self, url):
        """Normalize URL for consistent duplicate checking."""
        if not url: return ""
        url = url.strip().lower()
        if url.endswith('/'): url = url[:-1]
        if url.startswith('http://'): url = 'https://' + url[7:]
        if '?' in url: url = url.split('?')[0]
        return url

    def is_logical_duplicate(self, bank_code, title, date):
        """Check if a speech with same title and date already exists for this bank."""
        conn = self.db._get_conn()
        try:
            # Using EXACT match for title but stripped
            row = conn.execute(
                "SELECT id FROM speeches WHERE bank_code = ? AND title = ? AND date = ?",
                (bank_code, title.strip(), date)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """
        Main collection method: fetch new speeches and save to DB.
        Returns count of new speeches added.
        """
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        norm_existing_urls = {self.normalize_url(u) for u in existing_urls}
        
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            url = self.normalize_url(speech_info['url'])
            if url in norm_existing_urls:
                continue
                
            # Pre-emptive logical duplicate check
            if self.is_logical_duplicate(self.BANK_CODE, speech_info['title'], speech_info['date']):
                continue

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(speech_info['url'])
                
                # Check for embedded metadata from specific scrapers
                if full_text:
                    lines = full_text.split("\n")
                    new_text_lines = []
                    for line in lines:
                        if line.startswith("__DATE__:"):
                            speech_info['date'] = line.replace("__DATE__:", "").strip()
                        elif line.startswith("__SPEAKER__:"):
                            speech_info['speaker'] = line.replace("__SPEAKER__:", "").strip()
                        else:
                            new_text_lines.append(line)
                    full_text = "\n".join(new_text_lines).strip()

                if full_text:
                    logger.info(f"[{self.BANK_CODE}] Fetched: {speech_info['title'][:60]}...")

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=speech_info['url'], # Keep original URL for storage
                full_text=full_text,
            )

            if speech_id:
                new_count += 1

        logger.info(f"[{self.BANK_CODE}] Collection complete: {new_count} new speeches added")
        return new_count

    def collect_recent(self, fetch_text=True):
        """Collect only the current year's speeches (for daily runs)."""
        current_year = datetime.now().year
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        norm_existing_urls = {self.normalize_url(u) for u in existing_urls}

        speech_list = self.fetch_speech_list(year=current_year)
        if not speech_list:
            return 0

        new_count = 0
        for speech_info in speech_list:
            url = self.normalize_url(speech_info['url'])
            if url in norm_existing_urls:
                continue
            
            if self.is_logical_duplicate(self.BANK_CODE, speech_info['title'], speech_info['date']):
                continue

            full_text = speech_info.pop('_full_text', None)
            if fetch_text and not full_text:
                full_text = self.fetch_speech_text(speech_info['url'])

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=speech_info['url'],
                full_text=full_text,
            )
            if speech_id:
                new_count += 1

        return new_count
