"""Bank of Japan speech scraper."""

import logging
import re
from datetime import datetime

from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOJScraper(BaseScraper):
    """Collect English-language Bank of Japan speeches."""

    BANK_CODE = "BOJ"
    BANK_NAME = "Bank of Japan"
    BASE_URL = "https://www.boj.or.jp"

    def fetch_speech_list(self, year=None):
        """Fetch BOJ speeches for one year."""
        year_str = str(year) if year else str(datetime.now().year)
        url = (
            f"{self.BASE_URL}/en/about/press/"
            f"koen_{year_str}/index.htm"
        )
        resp = self._get(url)
        if not resp:
            raise RuntimeError(f"Failed to fetch BOJ speech list: {url}")

        soup = self._parse_html(resp.text)
        speeches = []

        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            link = cells[2].find("a", href=True)
            if not link:
                continue

            title = link.get_text(" ", strip=True)
            href = link["href"]
            if not self._is_speech_href(href) or not title:
                continue

            date = self._parse_boj_date(
                cells[0].get_text(" ", strip=True)
            ) or f"{year_str}-01-01"
            speaker = self._parse_speaker(
                cells[1].get_text(" ", strip=True)
            )
            speeches.append(
                {
                    "title": title,
                    "date": date,
                    "url": self._absolute_url(href),
                    "speaker": speaker,
                }
            )

        if speeches:
            return self._dedupe_speeches(speeches)

        for container in soup.find_all(["li", "td"]):
            link = container.find("a", href=True)
            if not link:
                continue

            title = link.get_text(strip=True)
            href = link["href"]
            if not title or len(title) < 5:
                continue
            if not self._is_speech_href(href):
                continue

            date = self._parse_boj_date(
                container.get_text(strip=True)
            )
            if not date and container.name == "td":
                previous = container.find_previous_sibling("td")
                if previous:
                    date = self._parse_boj_date(
                        previous.get_text(strip=True)
                    )

            speaker = None
            match = re.search(
                r"\((?:Speech|Remarks|Address)\s+by\s+"
                r"(?:Governor\s+|Deputy Governor\s+)?([^)]+)\)",
                title,
                re.IGNORECASE,
            )
            if match:
                speaker = match.group(1).strip()

            speeches.append(
                {
                    "title": title,
                    "date": date or f"{year_str}-01-01",
                    "url": self._absolute_url(href),
                    "speaker": speaker,
                }
            )

        return self._dedupe_speeches(speeches)

    def _absolute_url(self, href):
        if href.startswith("/"):
            return f"{self.BASE_URL}{href}"
        return href

    @staticmethod
    def _is_speech_href(href):
        """Return True for BOJ speech-detail links."""
        if not href:
            return False
        if (
            href.endswith("index.htm")
            or "r_menu" in href
            or "koen_all" in href
        ):
            return False
        return "/koen_" in href or "koen" in href

    @staticmethod
    def _parse_speaker(text):
        """Convert BOJ surname-first labels to a readable English name."""
        if not text:
            return None

        name = text.split(",", 1)[0].strip()
        parts = name.split()
        if len(parts) >= 2 and parts[0].isupper():
            surname = parts[0].title()
            given = " ".join(
                part.title() if part.isupper() else part
                for part in parts[1:]
            )
            return f"{given} {surname}"
        return name

    @staticmethod
    def _dedupe_speeches(speeches):
        """Collapse summary/full-text variants of the same speaker event."""
        key_map = {}
        for speech in speeches:
            clean_title = re.sub(
                r"\s*\((?:Summary|Outline|Full Text)\)",
                "",
                speech["title"],
                flags=re.IGNORECASE,
            ).strip().lower()
            key = (
                clean_title,
                speech["date"],
                speech.get("speaker") or "",
            )

            existing = key_map.get(key)
            if existing is None:
                key_map[key] = speech
                continue

            current_full = (
                "full text" in speech["title"].lower()
                or not speech["url"].lower().endswith(".pdf")
            )
            existing_full = (
                "full text" in existing["title"].lower()
                or not existing["url"].lower().endswith(".pdf")
            )
            if not existing_full and current_full:
                key_map[key] = speech

        return list(key_map.values())

    @staticmethod
    def _parse_boj_date(text):
        """Parse BOJ dates such as 'Mar. 3, 2026'."""
        if not text:
            return None

        text = text.replace("\xa0", " ")
        match = re.search(
            r"([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})",
            text,
        )
        if not match:
            return None

        date_text = match.group(1).replace(".", "")
        for fmt in ("%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(
                    date_text,
                    fmt,
                ).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_speech_text(self, url):
        """Fetch BOJ speech text and attach a detail-page speaker marker."""
        resp = self._get(url)
        if not resp:
            return None

        if self._is_pdf_response(url, resp):
            return self.extract_pdf_text(resp.content)

        try:
            soup = self._parse_html(resp.text)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to parse HTML for %s: %s",
                self.BANK_CODE,
                url,
                exc,
            )
            return None

        content = (
            soup.find("div", id="main")
            or soup.find("div", class_="section")
            or soup.find("main")
        )
        if not content:
            return None

        speaker = None
        first_paragraph = content.find("p")
        if first_paragraph:
            paragraph_text = first_paragraph.get_text(strip=True)
            match = re.search(
                r"^([^,]+?)\s+(?:Governor|Deputy Governor)",
                paragraph_text,
            )
            if match:
                speaker = match.group(1).strip()

        for tag in content.find_all(
            ["nav", "header", "footer", "script", "style", "aside"]
        ):
            tag.decompose()

        text = content.get_text(separator="\n", strip=True)
        if speaker:
            return f"__SPEAKER__:{speaker}\n{text}"
        return text

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch the maintained BOJ English archive from 2019 onward."""
        current_year = datetime.now().year
        start = start_year or 2019
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):
            try:
                speeches = self.fetch_speech_list(year=year)
            except RuntimeError as exc:
                logger.warning("[BOJ] %s", exc)
                continue
            if speeches:
                all_speeches.extend(speeches)
        return all_speeches
