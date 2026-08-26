"""European Central Bank speech scraper."""

import csv
import io
import logging
import re

from .base import BaseScraper


logger = logging.getLogger(__name__)


class ECBScraper(BaseScraper):
    BANK_CODE = "ECB"
    BANK_NAME = "European Central Bank"
    BASE_URL = "https://www.ecb.europa.eu"
    CSV_URL = (
        "https://www.ecb.europa.eu/press/key/shared/data/"
        "all_ECB_speeches.csv"
    )

    def fetch_speech_list(self, year=None):
        """Fetch ECB speeches from the CSV plus recent web entries."""
        speeches = self._fetch_from_csv(year)

        from datetime import datetime

        current_year = datetime.now().year
        if year is None or year >= current_year - 1:
            recent_html = self.fetch_recent_from_html()
            if recent_html:
                existing_urls = {speech["url"] for speech in speeches}
                for speech in recent_html:
                    if speech["url"] not in existing_urls:
                        speeches.append(speech)
                        existing_urls.add(speech["url"])
        return speeches

    def fetch_recent_speeches(self):
        """Fetch only recent ECB speeches from the RSS feed."""
        return self.fetch_recent_from_html()

    def _fetch_from_csv(self, year=None):
        response = self._get(self.CSV_URL)
        if not response:
            return []

        response.encoding = "utf-8"
        reader = csv.reader(io.StringIO(response.text), delimiter="|")
        header = next(reader, None)
        if not header:
            return []

        speeches = []
        for row in reader:
            if len(row) < 3:
                continue
            try:
                date_str = row[0].strip()
                speakers = row[1].strip() if len(row) > 1 else ""
                title = row[2].strip() if len(row) > 2 else ""
                subtitle = row[3].strip() if len(row) > 3 else ""
                contents = row[4].strip() if len(row) > 4 else ""

                date = self._parse_ecb_date(date_str)
                if not date:
                    continue
                if year and not date.startswith(str(year)):
                    continue

                url_slug = re.sub(
                    r"[^a-z0-9]+",
                    "-",
                    title.lower(),
                )[:60]
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

    def fetch_recent_from_html(self):
        """Fetch recent ECB speeches from the RSS feed.

        None means the RSS source could not be fetched or parsed. An empty
        list means the feed was parsed successfully but contained no current
        speech/interview entries.
        """
        url = "https://www.ecb.europa.eu/rss/press.html"
        response = self._get(url)
        if not response:
            return None

        try:
            import xml.etree.ElementTree as ET
            from datetime import datetime

            root = ET.fromstring(response.text)
            items = root.findall(".//item")
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

                pub_date_node = item.find("pubDate")
                date = ""
                if pub_date_node is not None and pub_date_node.text:
                    pub_date = pub_date_node.text.strip()
                    try:
                        parsed = datetime.strptime(
                            pub_date[:-6].strip(),
                            "%a, %d %b %Y %H:%M:%S",
                        )
                        date = parsed.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

                speaker = ""
                if "Speech by" in title:
                    speaker = (
                        title.split("Speech by")[-1]
                        .split(",")[0]
                        .strip()
                    )
                elif "Interview with" in title:
                    speaker = (
                        title.split("Interview with")[-1]
                        .split(",")[0]
                        .strip()
                    )

                speeches.append(
                    {
                        "title": title,
                        "date": date,
                        "url": href,
                        "speaker": speaker,
                    }
                )
        except Exception as exc:
            logger.error("[ECB] Error parsing RSS feed: %s", exc)
            return None

        logger.info(
            "[ECB] Found %s speeches from RSS feed",
            len(speeches),
        )
        return speeches

    def _parse_ecb_date(self, date_str):
        """Parse ECB date formats into YYYY-MM-DD."""
        from datetime import datetime

        formats = ["%Y-%m-%d", "%d/%m/%Y", "%d %B %Y", "%Y%m%d"]
        for date_format in formats:
            try:
                parsed = datetime.strptime(date_str.strip(), date_format)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_speech_text(self, url):
        """Fetch full text for ECB web entries."""
        if url.startswith("ecb://"):
            return None

        if self._is_pdf_response(url):
            response = self._get(url)
            if response and self._is_pdf_response(url, response):
                return self.extract_pdf_text(response.content)
            return None

        response = self._get(url)
        if not response:
            return None
        if self._is_pdf_response(url, response):
            return self.extract_pdf_text(response.content)

        soup = self._parse_html(response.text)
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

        if content:
            for tag in content.find_all(
                ["nav", "script", "style", "header", "footer"]
            ):
                tag.decompose()
            text = content.get_text(separator="\n", strip=True)
            if len(text) <= 100 and "Search Options" in text:
                return None
            return text
        return None

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """Collect ECB history, using CSV full text when available."""
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            url = speech_info["url"]
            if url in existing_urls:
                continue
            existing_urls.add(url)

            full_text = speech_info.pop("_full_text", None)
            if fetch_text and (
                not full_text or len(full_text) < 100
            ):
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
        """Collect recent ECB speeches from RSS without full CSV download."""
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.fetch_recent_speeches()
        if speech_list is None:
            raise RuntimeError("ECB recent RSS could not be fetched or parsed")

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
        """Return ECB speeches from its all-history dataset."""
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
