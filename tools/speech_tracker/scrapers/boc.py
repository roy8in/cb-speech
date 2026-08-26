"""Bank of Canada speech scraper."""

import logging
import re
from datetime import datetime, timezone

from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOCScraper(BaseScraper):
    """Collect text-based speeches and appearances from the Bank of Canada."""

    BANK_CODE = "BOC"
    BANK_NAME = "Bank of Canada"
    BASE_URL = "https://www.bankofcanada.ca"
    SPEECHES_URL = f"{BASE_URL}/press/speeches/"
    ALLOWED_CONTENT_TYPES = {
        "Comments",
        "Lectures",
        "Opening statements",
        "Presentations",
        "Remarks",
        "Speech summaries",
    }

    def fetch_speech_list(self, year=None):
        """Fetch the BOC speech list, following pagination."""
        all_speeches = []
        page = 1
        max_pages = 300

        while page <= max_pages:
            url = self.SPEECHES_URL
            if page > 1:
                url = f"{self.SPEECHES_URL}?mt_page={page}"

            resp = self._get(url)
            if not resp:
                raise RuntimeError(
                    f"Failed to fetch BOC speeches page {page}: {url}"
                )

            soup = self._parse_html(resp.text)
            speeches_on_page = self._parse_speech_list_page(soup)
            if not speeches_on_page:
                if page == 1:
                    raise RuntimeError(
                        "BOC speeches page returned no parseable results"
                    )
                break

            all_speeches.extend(speeches_on_page)
            if not self._has_next_page(soup):
                break
            page += 1

        if year:
            year_str = str(year)
            all_speeches = [
                speech
                for speech in all_speeches
                if speech["date"].startswith(year_str)
            ]

        unique = {speech["url"]: speech for speech in all_speeches}
        logger.info(
            "[%s] Found %s speeches",
            self.BANK_CODE,
            len(unique),
        )
        return list(unique.values())

    def _parse_speech_list_page(self, soup):
        """Parse one BOC result page."""
        speeches = []
        root = soup.find("main") or soup

        containers = root.find_all(
            ["div", "article"],
            class_=["media", "mtt-result"],
        )
        for container in containers:
            heading = container.find(["h3", "h5"])
            if not heading:
                continue

            link = heading.find("a", href=True)
            if not link:
                continue

            href = link["href"]
            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue
            if "/multimedia/" in href:
                continue

            content_types = self._extract_content_types(container)
            if not self._is_collectable_content_type(content_types):
                continue
            if not re.search(r"/\d{4}/\d{2}/", href):
                continue

            if href.startswith("/"):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith("http"):
                speech_url = href
            else:
                continue

            date = ""
            date_tag = container.find(
                "span",
                class_=["media-date", "pressdate"],
            )
            if date_tag:
                date = self._parse_boc_date(
                    date_tag.get_text(strip=True)
                )

            if not date:
                date_match = re.search(r"/(\d{4})/(\d{2})/", href)
                if date_match:
                    date = (
                        f"{date_match.group(1)}-"
                        f"{date_match.group(2)}-01"
                    )

            speaker = self._extract_speaker(heading)
            if not speaker:
                for separator in (":", "—", " - "):
                    if separator not in title:
                        continue
                    potential = title.split(separator, 1)[0].strip()
                    if 1 < len(potential.split()) < 5:
                        speaker = potential
                        break

            speeches.append(
                {
                    "title": title,
                    "date": date,
                    "url": speech_url,
                    "speaker": speaker,
                    "speech_type": self._primary_speech_type(
                        content_types
                    ),
                }
            )

        return speeches

    def _extract_content_types(self, container):
        """Return BOC content-type labels attached to a result card."""
        content_types = []
        for link in container.find_all("a"):
            label = link.get_text(" ", strip=True)
            if (
                label in self.ALLOWED_CONTENT_TYPES
                or label == "Webcasts"
            ):
                content_types.append(label)
        return content_types

    def _is_collectable_content_type(self, content_types):
        """Return True for text speech-like content, excluding webcasts."""
        if not content_types or "Webcasts" in content_types:
            return False
        return any(
            content_type in self.ALLOWED_CONTENT_TYPES
            for content_type in content_types
        )

    def _primary_speech_type(self, content_types):
        for content_type in content_types:
            if content_type in self.ALLOWED_CONTENT_TYPES:
                return content_type
        return "speech"

    @staticmethod
    def _parse_boc_date(date_text):
        """Parse common BOC publication-date formats."""
        if not date_text:
            return None

        date_text = date_text.replace("\xa0", " ").strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(
                    date_text,
                    fmt,
                ).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_speaker(heading):
        """Extract the speaker from the result card's profile link."""
        container = heading.find_parent(["div", "article"])
        if not container:
            return None

        profile_link = container.find(
            "a",
            href=re.compile(r"/profile/"),
        )
        if not profile_link:
            return None
        return profile_link.get_text(strip=True)

    @staticmethod
    def _has_next_page(soup):
        """Return True when BOC pagination exposes another page."""
        return bool(
            soup.find("a", class_="next")
            or soup.find("a", string=re.compile(r"Next|›|»"))
        )

    def fetch_speech_text(self, url):
        """Fetch and clean one BOC speech page."""
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
            soup.find("main")
            or soup.find("article")
            or soup.find("div", id="main-content")
        )
        if not content:
            return None

        for tag in content.find_all(
            ["nav", "header", "footer", "script", "style", "aside", "form"]
        ):
            tag.decompose()

        for tag in content.find_all(
            "div",
            class_=[
                "related-info",
                "media-sidebar",
                "sharing-tools",
                "cfct-sidebar",
            ],
        ):
            tag.decompose()

        text = content.get_text(separator="\n", strip=True)
        text = self._clean_speech_text(text)

        if "About us" in text[:200]:
            real_start = content.find(["h1", "h2", "h3"])
            if real_start:
                text = "\n".join(
                    sibling.strip()
                    for sibling in real_start.find_all_next(string=True)
                    if sibling.strip()
                )

        return self._clean_speech_text(text)

    def collect_recent(self, fetch_text=True):
        """Collect current-year BOC speeches and refresh changed rows."""
        current_year = datetime.now().year
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        existing_by_normalized_url = {
            self.normalize_url(url): url for url in existing_urls
        }

        speech_list = self.fetch_speech_list(year=current_year)
        if not speech_list:
            existing_count = self.db.count_speeches_for_year(
                self.BANK_CODE,
                current_year,
            )
            if existing_count > 0:
                raise RuntimeError(
                    "BOC scraper returned 0 speeches for "
                    f"{current_year}, but SQLite already contains "
                    f"{existing_count}."
                )
            return 0

        new_count = 0
        refreshed_count = 0
        for speech_info in speech_list:
            normalized_url = self.normalize_url(speech_info["url"])
            stored_url = existing_by_normalized_url.get(normalized_url)
            if stored_url:
                if self._refresh_existing_speech(
                    speech_info,
                    stored_url=stored_url,
                    fetch_text=fetch_text,
                ):
                    refreshed_count += 1
                continue

            existing_by_normalized_url[normalized_url] = speech_info["url"]
            full_text = None
            if fetch_text:
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

        if refreshed_count:
            logger.info(
                "[%s] Refreshed %s existing recent speeches",
                self.BANK_CODE,
                refreshed_count,
            )
        return new_count

    def _refresh_existing_speech(
        self,
        speech_info,
        stored_url,
        fetch_text=True,
    ):
        """Refresh an existing BOC row when its source page changed."""
        conn = self.db._get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    s.id,
                    s.title,
                    s.date,
                    s.speech_type,
                    s.full_text,
                    m.name AS speaker
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                WHERE s.url = ?
                """,
                (stored_url,),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            return False

        desired_type = speech_info.get("speech_type", "speech")
        desired_speaker = speech_info.get("speaker")
        dirty_text = self._looks_like_dirty_boc_text(row["full_text"])
        metadata_changed = (
            row["title"] != speech_info["title"]
            or row["date"] != speech_info["date"]
            or row["speech_type"] != desired_type
            or row["speaker"] != desired_speaker
        )
        if not metadata_changed and not dirty_text:
            return False

        full_text = row["full_text"]
        if fetch_text:
            fetched_text = self.fetch_speech_text(speech_info["url"])
            if fetched_text:
                full_text = fetched_text

        content_changed = (
            full_text != row["full_text"]
            or speech_info["date"] != row["date"]
        )
        if content_changed:
            self.db.update_speech_content(
                row["id"],
                full_text,
                speech_info["date"],
            )

        speaker_id = self.db.get_or_create_member(
            self.BANK_CODE,
            desired_speaker,
        )
        conn = self.db._get_conn()
        try:
            conn.execute(
                """
                UPDATE speeches
                SET title = ?,
                    speaker_id = ?,
                    speech_type = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    speech_info["title"],
                    speaker_id,
                    desired_type,
                    datetime.now(timezone.utc).isoformat(),
                    row["id"],
                ),
            )
            conn.commit()
        finally:
            conn.close()

        return True

    @staticmethod
    def _looks_like_dirty_boc_text(text):
        if not text:
            return False
        return any(
            marker in text
            for marker in (
                "Share this page",
                "Content Type(s)",
                "Related Information",
            )
        )

    @staticmethod
    def _clean_speech_text(text):
        """Remove BOC page chrome while retaining speech content."""
        if not text:
            return text

        lines = []
        previous = None
        for raw_line in text.replace("\xa0", " ").splitlines():
            line = raw_line.strip()
            if not line or line == previous:
                continue
            if line.startswith("Share this page"):
                continue
            if line in {"Available as:", "PDF", "Audio", "Video"}:
                continue
            lines.append(line)
            previous = line

        cut_markers = {
            "Content Type(s)",
            "Subject(s)",
            "Related Information",
        }
        for index, line in enumerate(lines):
            if line in cut_markers:
                lines = lines[:index]
                break

        return "\n".join(lines).strip()

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch the paginated BOC archive and filter by year range."""
        all_speeches = self.fetch_speech_list()

        if start_year:
            all_speeches = [
                speech
                for speech in all_speeches
                if speech["date"] >= f"{start_year}-01-01"
            ]
        if end_year:
            all_speeches = [
                speech
                for speech in all_speeches
                if speech["date"] <= f"{end_year}-12-31"
            ]
        return all_speeches
