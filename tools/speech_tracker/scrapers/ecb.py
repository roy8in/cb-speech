"""European Central Bank speech scraper."""

import csv
import io
import logging
import re
from email.utils import parsedate_to_datetime

from .base import BaseScraper

logger = logging.getLogger(__name__)


class ECBScraper(BaseScraper):
    """Collect ECB speeches from the official dataset and RSS feed."""

    BANK_CODE = "ECB"
    BANK_NAME = "European Central Bank"
    BASE_URL = "https://www.ecb.europa.eu"
    CSV_URL = (
        "https://www.ecb.europa.eu/press/key/shared/data/"
        "all_ECB_speeches.csv"
    )
    RSS_URL = "https://www.ecb.europa.eu/rss/press.html"

    def fetch_speech_list(self, year=None):
        """Fetch historical/full speech data from the official ECB CSV."""
        return self._fetch_from_csv(year=year)

    def fetch_recent_speeches(self):
        """Fetch recent ECB speeches and interviews from the RSS feed."""
        return self._fetch_from_rss()

    def _fetch_from_csv(self, year=None):
        """Read the ECB pipe-delimited speeches dataset."""
        resp = self._get(self.CSV_URL)
        if not resp:
            raise RuntimeError("Failed to fetch ECB speeches CSV")

        resp.encoding = "utf-8"
        reader = csv.reader(io.StringIO(resp.text), delimiter="|")
        header = next(reader, None)
        if not header:
            raise RuntimeError("ECB speeches CSV has no header")

        speeches = []
        for row in reader:
            if len(row) < 3:
                continue
            try:
                date = self._parse_ecb_date(row[0].strip())
                if not date:
                    continue
                if year and not date.startswith(str(year)):
                    continue

                speakers = row[1].strip() if len(row) > 1 else ""
                title = row[2].strip() if len(row) > 2 else ""
                subtitle = row[3].strip() if len(row) > 3 else ""
                contents = row[4].strip() if len(row) > 4 else ""
                if not title:
                    continue

                url_slug = re.sub(
                    r"[^a-z0-9]+",
                    "-",
                    title.lower(),
                ).strip("-")[:60]
                url = f"ecb://speeches/{date}/{url_slug}"
                full_title = (
                    f"{title} - {subtitle}" if subtitle else title
                )

                speeches.append(
                    {
                        "title": full_title,
                        "date": date,
                        "url": url,
                        "speaker": speakers,
                        "_full_text": contents,
                    }
                )
            except Exception as exc:
                logger.warning("[ECB] Error parsing CSV row: %s", exc)

        return speeches

    def _fetch_from_rss(self):
        """Parse the ECB press RSS feed and keep speech-like items."""
        resp = self._get(self.RSS_URL)
        if not resp:
            return None

        try:
            import xml.etree.ElementTree as ET

            root = ET.fromstring(resp.text)
            items = root.findall(".//item")
        except Exception as exc:
            logger.error("[ECB] Error parsing RSS feed: %s", exc)
            return None

        if not items:
            logger.error("[ECB] RSS parsed but contained no items")
            return None

        speeches = []
        for item in items:
            link_node = item.find("link")
            if link_node is None or not link_node.text:
                continue

            href = link_node.text.strip()
            if (
                "/press/key/" not in href
                and "/press/inter/" not in href
            ):
                continue

            title_node = item.find("title")
            title = (
                title_node.text.strip()
                if title_node is not None and title_node.text
                else ""
            )
            if not title:
                continue

            date = self._parse_rss_date(item.find("pubDate"))
            if not date:
                logger.warning(
                    "[ECB] Skipping RSS item without a valid date: %s",
                    title,
                )
                continue

            speeches.append(
                {
                    "title": title,
                    "date": date,
                    "url": href,
                    "speaker": self._extract_rss_speaker(title),
                }
            )

        logger.info(
            "[ECB] Found %s speeches/interviews from RSS feed",
            len(speeches),
        )
        return speeches

    @staticmethod
    def _parse_rss_date(pub_date_node):
        if pub_date_node is None or not pub_date_node.text:
            return None
        try:
            return parsedate_to_datetime(
                pub_date_node.text.strip()
            ).date().isoformat()
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_rss_speaker(title):
        """Extract speaker labels used by current ECB RSS titles."""
        if not title:
            return ""

        if "Speech by" in title:
            return title.split("Speech by", 1)[1].split(",", 1)[0].strip()
        if "Interview with" in title:
            return (
                title.split("Interview with", 1)[1]
                .split(",", 1)[0]
                .strip()
            )
        if ":" in title:
            prefix = title.split(":", 1)[0].strip()
            if 1 <= len(prefix.split()) <= 8:
                return prefix
        return ""

    @staticmethod
    def _parse_ecb_date(date_text):
        """Parse supported ECB dataset date formats."""
        from datetime import datetime

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %B %Y", "%Y%m%d"):
            try:
                return datetime.strptime(
                    date_text.strip(),
                    fmt,
                ).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_speech_text(self, url):
        """Fetch a recent ECB speech/interview page."""
        if url.startswith("ecb://"):
            return None

        if self._is_pdf_response(url):
            resp = self._get(url)
            if resp and self._is_pdf_response(url, resp):
                return self.extract_pdf_text(resp.content)
            return None

        resp = self._get(url)
        if not resp:
            return None
        if self._is_pdf_response(url, resp):
            return self.extract_pdf_text(resp.content)

        soup = self._parse_html(resp.text)
        content = soup.find("main") or soup.find("article")
        if not content:
            sections = soup.find_all("div", class_="section")
            content = max(
                sections,
                key=lambda tag: len(
                    tag.get_text(separator="\n", strip=True)
                ),
                default=None,
            )
        if not content:
            return None

        for tag in content.find_all(
            ["nav", "script", "style", "header", "footer"]
        ):
            tag.decompose()
        text = content.get_text(separator="\n", strip=True)
        if len(text) <= 100 and "Search Options" in text:
            return None
        return text

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """Load the official ECB CSV archive without mixing in RSS rows."""
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            url = speech_info["url"]
            if url in existing_urls:
                continue
            existing_urls.add(url)

            full_text = speech_info.get("_full_text")
            if fetch_text and (not full_text or len(full_text) < 100):
                web_text = self.fetch_speech_text(url)
                if web_text:
                    full_text = web_text

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get("speaker"),
                title=speech_info["title"],
                date=speech_info["date"],
                url=url,
                full_text=full_text,
            )
            if speech_id:
                new_count += 1

        logger.info(
            "[ECB] Collection complete: %s new speeches added",
            new_count,
        )
        return new_count

    def collect_recent(self, fetch_text=True):
        """Collect only the newest RSS items for daily operation."""
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.fetch_recent_speeches()
        if speech_list is None:
            raise RuntimeError(
                "ECB recent RSS could not be fetched or parsed"
            )

        new_count = 0
        for speech_info in speech_list:
            url = speech_info["url"]
            if url in existing_urls:
                continue
            existing_urls.add(url)

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(url)

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get("speaker"),
                title=speech_info["title"],
                date=speech_info["date"],
                url=url,
                full_text=full_text,
            )
            if speech_id:
                new_count += 1

        logger.info(
            "[ECB] Recent collection complete: %s new speeches added",
            new_count,
        )
        return new_count

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch the ECB CSV once, then filter the requested range."""
        speeches = self.fetch_speech_list()
        if start_year:
            speeches = [
                speech
                for speech in speeches
                if speech["date"] >= f"{start_year}-01-01"
            ]
        if end_year:
            speeches = [
                speech
                for speech in speeches
                if speech["date"] <= f"{end_year}-12-31"
            ]
        return speeches
