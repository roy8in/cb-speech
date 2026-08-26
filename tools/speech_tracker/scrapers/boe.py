"""Bank of England speech scraper."""

import logging
import re
from datetime import datetime

from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOEScraper(BaseScraper):
    """Collect Bank of England speeches."""

    BANK_CODE = "BOE"
    BANK_NAME = "Bank of England"
    BASE_URL = "https://www.bankofengland.co.uk"

    def fetch_speech_list(self, year=None):
        """Fetch BOE speeches for one year."""
        current_year = datetime.now().year
        if year is None or year >= current_year - 1:
            url = f"{self.BASE_URL}/news/speeches"
            html = self._get_playwright(url)
        else:
            url = f"{self.BASE_URL}/sitemap/speeches"
            resp = self._get(url)
            html = resp.text if resp else None

        if not html:
            if "/news/" in url:
                fallback_url = f"{self.BASE_URL}/sitemap/speeches"
                resp = self._get(fallback_url)
                html = resp.text if resp else None
            else:
                fallback_url = f"{self.BASE_URL}/news/speeches"
                html = self._get_playwright(fallback_url)

        if not html:
            raise RuntimeError("Failed to load BOE speech listings")

        soup = self._parse_html(html)
        speeches = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            heading = link.find("h3", class_="list")
            title = (
                heading.get_text(strip=True)
                if heading
                else link.get_text(strip=True)
            )

            if not title or len(title) < 10:
                continue
            if not any(
                pattern in href
                for pattern in ("/speech/", "/speeches/")
            ):
                continue
            if href in ("/sitemap/speeches", "/news/speeches"):
                continue

            if href.startswith("/"):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith("http"):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/{href}"

            normalized_url = self.normalize_url(speech_url)
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)

            time_tag = link.find("time", class_="release-date")
            if time_tag and time_tag.get("datetime"):
                date = time_tag["datetime"][:10]
            else:
                date = self._extract_date_from_url(href, year)

            if year and date and not date.startswith(str(year)):
                continue

            speaker = None
            tag = link.find("div", class_="release-tag")
            if tag:
                tag_text = tag.get_text(strip=True)
                if "//" in tag_text:
                    speaker = tag_text.split("//")[-1].strip()

            if not speaker:
                speaker = self.extract_speaker_from_title(title)

            speeches.append(
                {
                    "title": title,
                    "date": date,
                    "url": speech_url,
                    "speaker": speaker,
                }
            )

        return speeches

    @staticmethod
    def extract_speaker_from_title(title):
        """Extract a likely speaker name from common BOE title patterns."""
        clean_title = re.sub(
            r"\(pdf\s*.*\)",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip()

        match = re.search(
            r".+[−–-]\s*(?:speech|remarks|slides|panel remarks|address)"
            r"\s+by\s+([^−–-]+)$",
            clean_title,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()

        if ":" in clean_title:
            potential = clean_title.split(":", 1)[0].strip()
            blocked_words = ("at", "the", "meeting", "update")
            if (
                1 < len(potential.split()) < 5
                and not any(
                    word in potential.lower()
                    for word in blocked_words
                )
            ):
                return potential

        match = re.search(
            r"Slides\s+from\s+([^’']+)[’']s",
            clean_title,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()
        return None

    @staticmethod
    def _extract_date_from_url(href, default_year):
        """Extract an approximate date from BOE URL paths."""
        match = re.search(r"/(\d{4})/(\w+)/", href)
        if match:
            year = match.group(1)
            month_text = match.group(2).title()
            for fmt in ("%B", "%b"):
                try:
                    month = datetime.strptime(month_text, fmt).month
                    return f"{year}-{month:02d}-01"
                except ValueError:
                    continue

        match = re.search(r"/(\d{4})/", href)
        if match:
            return f"{match.group(1)}-01-01"
        if default_year:
            return f"{default_year}-01-01"
        return ""

    def fetch_speech_text(self, url):
        """Fetch the speech text and attach an exact date when found."""
        content_type = ""
        try:
            head_resp = self.session.head(
                url,
                timeout=self.REQUEST_TIMEOUT,
                verify=False,
            )
            content_type = head_resp.headers.get(
                "Content-Type",
                "",
            ).lower()
        except Exception:
            pass

        if self._is_pdf_response(url, content_type=content_type):
            resp = self._get(url)
            if not resp:
                return None
            text = self.extract_pdf_text(resp.content)
            exact_date = self._extract_date_from_text(text)
            if exact_date:
                return f"__DATE__:{exact_date}\n{text}"
            return text

        html = self._get_playwright(url)
        if not html:
            logger.info(
                "[%s] Playwright failed, using standard request for %s",
                self.BANK_CODE,
                url,
            )
            resp = self._get(url)
            html = resp.text if resp else None
        if not html:
            return None

        try:
            soup = self._parse_html(html)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to parse HTML for %s: %s",
                self.BANK_CODE,
                url,
                exc,
            )
            return None

        exact_date = self._extract_page_date(soup)
        content = (
            soup.find("div", class_="page-content")
            or soup.find("article")
            or soup.find("div", class_="content-block")
            or soup.find("main")
            or soup.find("body")
        )
        if not content:
            return None

        if not exact_date:
            raw_start = content.get_text(
                separator=" ",
                strip=True,
            )[:1000]
            match = re.search(
                r"(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})",
                raw_start,
            )
            if match:
                try:
                    exact_date = datetime.strptime(
                        match.group(1),
                        "%d %B %Y",
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    pass

        for tag in content.find_all(
            ["nav", "header", "footer", "script", "style", "aside", "button"]
        ):
            tag.decompose()
        text = content.get_text(separator="\n", strip=True)

        if exact_date:
            return f"__DATE__:{exact_date}\n{text}"
        return text

    @staticmethod
    def _extract_page_date(soup):
        """Extract the publication date from BOE page metadata."""
        meta_date = (
            soup.find(
                "meta",
                attrs={"property": "article:published_time"},
            )
            or soup.find("meta", attrs={"name": "date"})
            or soup.find(
                "meta",
                attrs={"property": "og:article:published_time"},
            )
        )
        if meta_date and meta_date.get("content"):
            return meta_date["content"][:10]

        date_element = soup.find("div", class_="published-date")
        if date_element:
            date_text = date_element.get_text(strip=True).replace(
                "Published on",
                "",
            ).strip()
            try:
                return datetime.strptime(
                    date_text,
                    "%d %B %Y",
                ).strftime("%Y-%m-%d")
            except ValueError:
                pass

        time_element = soup.find("time")
        if time_element and time_element.get("datetime"):
            return time_element["datetime"][:10]
        return None

    @staticmethod
    def _extract_date_from_text(text):
        """Extract a BOE-style date from PDF text."""
        if not text:
            return None

        raw_start = text[:2500]
        patterns = (
            r"\b(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})\b",
            r"\b([A-Z][a-z]+\s+\d{1,2},\s+\d{4})\b",
        )
        for pattern in patterns:
            match = re.search(pattern, raw_start)
            if not match:
                continue
            for fmt in ("%d %B %Y", "%B %d, %Y"):
                try:
                    return datetime.strptime(
                        match.group(1),
                        fmt,
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    continue
        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch BOE speeches year by year instead of only the latest page."""
        return super().get_all_speeches(
            start_year=start_year or 2000,
            end_year=end_year,
        )
