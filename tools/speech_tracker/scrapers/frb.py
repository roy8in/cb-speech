"""Federal Reserve Board speech scraper."""

import logging
import re
from datetime import datetime

from .base import BaseScraper

logger = logging.getLogger(__name__)


class FRBScraper(BaseScraper):
    """Collect speeches from the Federal Reserve Board website."""

    BANK_CODE = "FRB"
    BANK_NAME = "Federal Reserve"
    BASE_URL = "https://www.federalreserve.gov"

    def _get_year_url(self, year):
        """Return the Federal Reserve speech-list URL for one year."""
        if year >= 2011:
            return f"{self.BASE_URL}/newsevents/{year}-speeches.htm"
        return f"{self.BASE_URL}/newsevents/{year}speech.htm"

    def _lookup_speaker(self, last_name):
        """Resolve a URL-derived surname against known FRB members."""
        if not last_name:
            return None

        conn = self.db._get_conn()
        try:
            row = conn.execute(
                """
                SELECT name
                FROM members
                WHERE bank_code = 'FRB'
                  AND (name LIKE ? OR name LIKE ?)
                ORDER BY status = 'active' DESC,
                         last_speech_date DESC
                LIMIT 1
                """,
                (f"% {last_name}", f"{last_name}%"),
            ).fetchone()
            return row["name"] if row else last_name
        finally:
            conn.close()

    @staticmethod
    def _speaker_from_context(link):
        """Read the full speaker label printed beside a speech-list item."""
        container = link.find_parent(["div", "li", "article"])
        if not container:
            container = link.parent
        if not container:
            return None

        prefixes = (
            "Vice Chair for Supervision ",
            "Vice Chair ",
            "Chair ",
            "Governor ",
        )
        lines = container.get_text("\n", strip=True).splitlines()
        for line in lines:
            clean_line = " ".join(line.split())
            for prefix in prefixes:
                if clean_line.startswith(prefix):
                    speaker = clean_line[len(prefix):].strip()
                    if speaker:
                        return speaker
        return None

    def fetch_speech_list(self, year=None):
        """Fetch Federal Reserve speeches for one year."""
        if year is None:
            year = datetime.now().year

        url = self._get_year_url(year)
        resp = self._get(url)
        if not resp:
            raise RuntimeError(f"Failed to fetch FRB speech list: {url}")

        soup = self._parse_html(resp.text)
        speeches = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            title = link.get_text(strip=True)
            if "/newsevents/speech/" not in href:
                continue
            if not title or len(title) < 10:
                continue
            if title.lower() in (
                "speech",
                "speeches",
                "archive",
                "more",
            ):
                continue

            if href.startswith("/"):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith("http"):
                speech_url = href
            else:
                speech_url = (
                    f"{self.BASE_URL}/newsevents/speech/{href}"
                )

            normalized_url = self.normalize_url(speech_url)
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)

            date_match = re.search(r"(\d{8})", href)
            if date_match:
                date_text = date_match.group(1)
                date = (
                    f"{date_text[:4]}-{date_text[4:6]}-"
                    f"{date_text[6:8]}"
                )
            else:
                date = f"{year}-01-01"

            speaker = self._speaker_from_context(link)
            if not speaker:
                speaker_match = re.search(
                    r"/speech/([a-z]+)\d{8}",
                    href,
                )
                if speaker_match:
                    speaker = self._lookup_speaker(
                        speaker_match.group(1).title()
                    )

            speeches.append(
                {
                    "title": title,
                    "date": date,
                    "url": speech_url,
                    "speaker": speaker,
                }
            )

        return speeches

    def fetch_speech_text(self, url):
        """Fetch the full text of a Federal Reserve speech."""
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
            soup.find("div", class_="col-xs-12 col-sm-8 col-md-8")
            or soup.find("div", id="article")
            or soup.find("div", class_="article")
            or soup.find("main")
            or soup.find("article")
        )
        if not content:
            return None

        for tag in content.find_all(
            ["nav", "header", "footer", "script", "style"]
        ):
            tag.decompose()
        return content.get_text(separator="\n", strip=True)

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch the maintained Federal Reserve archive from 2006."""
        return super().get_all_speeches(
            start_year=start_year or 2006,
            end_year=end_year,
        )
