"""Base scraper class for central-bank speeches."""

import atexit
import asyncio
import concurrent.futures
import io
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for central-bank speech scrapers."""

    BANK_CODE = None
    BANK_NAME = None
    BASE_URL = None

    REQUEST_DELAY = 0.5
    REQUEST_TIMEOUT = 30
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
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
            response = self.session.get(
                url,
                timeout=self.REQUEST_TIMEOUT,
                verify=False,
                **kwargs,
            )
            response.raise_for_status()
            if (
                response.encoding is None
                or response.encoding.lower() == "iso-8859-1"
            ):
                response.encoding = response.apparent_encoding or "utf-8"
            return response
        except requests.RequestException as exc:
            logger.error(
                "[%s] Request failed for %s: %s",
                self.BANK_CODE,
                url,
                exc,
            )
            return None

    def _parse_html(self, html_text):
        """Parse HTML with BeautifulSoup."""
        return BeautifulSoup(html_text, "html.parser")

    def extract_pdf_text(self, pdf_bytes):
        """Extract text from a PDF file using pdfplumber."""
        try:
            import pdfplumber
        except ImportError:
            logger.error("pdfplumber is not installed")
            return "Error: pdfplumber not installed. Cannot extract PDF text."

        text_pages = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        text_pages.append(text)

            full_text = "\n\n".join(text_pages).strip()

            import re

            full_text = re.sub(r"(\w+)-\n([a-z]+)", r"\1\2", full_text)
            if not full_text or len(full_text) < 50:
                return (
                    "This speech appears to be a scanned document or "
                    "contains no extractable text."
                )
            return full_text
        except Exception as exc:
            logger.error(
                "[%s] Failed to parse PDF: %s",
                self.BANK_CODE,
                exc,
            )
            return "Error: Failed to extract text from this PDF document."

    def _is_pdf_response(self, url, resp=None, content_type=None):
        """Return True when a URL or HTTP response points to a PDF."""
        if url and url.lower().split("?", 1)[0].endswith(".pdf"):
            return True
        if content_type is None and resp is not None:
            content_type = resp.headers.get("Content-Type", "")
        return "application/pdf" in (content_type or "").lower()

    def _get_playwright_in_thread(self, url, wait_ms):
        """Run sync Playwright in a worker thread."""
        from playwright.sync_api import sync_playwright

        try:
            with sync_playwright() as manager:
                browser = manager.chromium.launch(headless=True)
                try:
                    context = browser.new_context(
                        user_agent=self.HEADERS["User-Agent"]
                    )
                    page = context.new_page()
                    try:
                        page.goto(
                            url,
                            wait_until="networkidle",
                            timeout=self.REQUEST_TIMEOUT * 1000,
                        )
                        if wait_ms > 0:
                            time.sleep(wait_ms / 1000)
                        return page.content()
                    finally:
                        page.close()
                        context.close()
                finally:
                    browser.close()
        except Exception as exc:
            logger.error(
                "[%s] Playwright failed for %s: %s",
                self.BANK_CODE,
                url,
                exc,
            )
            return None

    def _get_playwright_sync(self, url, wait_ms):
        """Use a reusable sync Playwright browser."""
        from playwright.sync_api import sync_playwright

        try:
            if self._playwright_manager is None:
                self._playwright_manager = sync_playwright().start()
                self._playwright_browser = (
                    self._playwright_manager.chromium.launch(headless=True)
                )
                self._playwright_context = (
                    self._playwright_browser.new_context(
                        user_agent=self.HEADERS["User-Agent"]
                    )
                )

            page = self._playwright_context.new_page()
            try:
                page.goto(
                    url,
                    wait_until="networkidle",
                    timeout=self.REQUEST_TIMEOUT * 1000,
                )
                if wait_ms > 0:
                    time.sleep(wait_ms / 1000)
                return page.content()
            finally:
                page.close()
        except Exception as exc:
            logger.error(
                "[%s] Playwright failed for %s: %s",
                self.BANK_CODE,
                url,
                exc,
            )
            self._close_playwright()
            return None

    def _get_playwright(self, url, wait_ms=2000):
        """Use Playwright to get page content."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self._get_playwright_sync(url, wait_ms)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=1
        ) as executor:
            return executor.submit(
                self._get_playwright_in_thread,
                url,
                wait_ms,
            ).result()

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
        """Fetch speech metadata for a year."""
        pass

    @abstractmethod
    def fetch_speech_text(self, url):
        """Fetch the full text of one speech."""
        pass

    def refresh_incomplete_speeches(self):
        """Re-fetch recent speeches with missing or incomplete text."""
        incomplete = self.db.get_incomplete_speeches(self.BANK_CODE)
        if not incomplete:
            return 0

        refreshed_count = 0
        for item in incomplete:
            logger.info(
                "[%s] Refreshing: %s (%s)",
                self.BANK_CODE,
                item["title"],
                item["url"],
            )
            full_text = self.fetch_speech_text(item["url"])
            if not full_text:
                continue

            exact_date = None
            if full_text.startswith("__DATE__:"):
                parts = full_text.split("\n", 1)
                exact_date = parts[0].replace("__DATE__:", "").strip()
                full_text = parts[1].strip() if len(parts) > 1 else ""

            if (
                len(full_text) > 500
                and "to be published" not in full_text.lower()
                and self.db.update_speech_content(
                    item["id"],
                    full_text,
                    exact_date,
                )
            ):
                refreshed_count += 1
                logger.info(
                    "[%s] Successfully refreshed ID %s",
                    self.BANK_CODE,
                    item["id"],
                )

        return refreshed_count

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch all available speeches across years."""
        current_year = datetime.now().year
        start = start_year or 2000
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):
            try:
                speeches = self.fetch_speech_list(year=year)
                if speeches:
                    all_speeches.extend(speeches)
                    logger.info(
                        "[%s] %s: %s speeches found",
                        self.BANK_CODE,
                        year,
                        len(speeches),
                    )
            except Exception as exc:
                logger.warning(
                    "[%s] Failed to fetch %s: %s",
                    self.BANK_CODE,
                    year,
                    exc,
                )
        return all_speeches

    def normalize_url(self, url):
        """Normalize a URL for duplicate checking."""
        if not url:
            return ""
        normalized = url.strip().lower()
        if normalized.endswith("/"):
            normalized = normalized[:-1]
        if normalized.startswith("http://"):
            normalized = "https://" + normalized[7:]
        if "?" in normalized:
            normalized = normalized.split("?", 1)[0]
        return normalized

    def _guard_recent_zero_result(self, speech_list, year):
        """Raise when a previously populated yearly source suddenly empties."""
        if speech_list:
            return
        existing_count = self.db.count_speeches_for_year(
            self.BANK_CODE,
            year,
        )
        if existing_count > 0:
            raise RuntimeError(
                f"{self.BANK_CODE} scraper returned 0 speeches for {year} "
                f"but SQLite already contains {existing_count}."
            )

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """Fetch and store speeches, using URL as the source identity."""
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        normalized_urls = {
            self.normalize_url(url) for url in existing_urls
        }
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            normalized_url = self.normalize_url(speech_info["url"])
            if normalized_url in normalized_urls:
                continue
            normalized_urls.add(normalized_url)

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(speech_info["url"])
                if full_text:
                    lines = full_text.split("\n")
                    clean_lines = []
                    for line in lines:
                        if line.startswith("__DATE__:"):
                            speech_info["date"] = line.replace(
                                "__DATE__:",
                                "",
                            ).strip()
                        elif line.startswith("__SPEAKER__:"):
                            speech_info["speaker"] = line.replace(
                                "__SPEAKER__:",
                                "",
                            ).strip()
                        else:
                            clean_lines.append(line)
                    full_text = "\n".join(clean_lines).strip()

                if full_text:
                    logger.info(
                        "[%s] Fetched: %s...",
                        self.BANK_CODE,
                        speech_info["title"][:60],
                    )

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get("speaker"),
                title=speech_info["title"],
                date=speech_info["date"],
                url=speech_info["url"],
                full_text=full_text,
                speech_type=speech_info.get("speech_type", "speech"),
            )
            if speech_id:
                new_count += 1

        logger.info(
            "[%s] Collection complete: %s new speeches added",
            self.BANK_CODE,
            new_count,
        )
        return new_count

    def collect_recent(self, fetch_text=True):
        """Collect the current year's speeches for daily runs."""
        current_year = datetime.now().year
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        normalized_urls = {
            self.normalize_url(url) for url in existing_urls
        }

        speech_list = self.fetch_speech_list(year=current_year)
        self._guard_recent_zero_result(speech_list, current_year)
        if not speech_list:
            return 0

        new_count = 0
        for speech_info in speech_list:
            normalized_url = self.normalize_url(speech_info["url"])
            if normalized_url in normalized_urls:
                continue
            normalized_urls.add(normalized_url)

            full_text = speech_info.pop("_full_text", None)
            if fetch_text and not full_text:
                full_text = self.fetch_speech_text(speech_info["url"])

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get("speaker"),
                title=speech_info["title"],
                date=speech_info["date"],
                url=speech_info["url"],
                full_text=full_text,
                speech_type=speech_info.get("speech_type", "speech"),
            )
            if speech_id:
                new_count += 1

        return new_count
