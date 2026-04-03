"""
European Central Bank (ECB) Speech Scraper

Source: ECB Speeches Dataset (CSV)
URL: https://www.ecb.europa.eu/press/key/html/downloads.en.html
CSV: pipe-delimited, UTF-8, monthly updates, includes full text
"""

import csv
import io
import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class ECBScraper(BaseScraper):
    BANK_CODE = 'ECB'
    BANK_NAME = 'European Central Bank'
    BASE_URL = 'https://www.ecb.europa.eu'
    CSV_URL = 'https://www.ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv'

    def fetch_speech_list(self, year=None):
        """
        Fetch all ECB speeches from the CSV dataset.
        If year is specified, filter to that year.
        """
        resp = self._get(self.CSV_URL)
        if not resp:
            return []

        # ECB CSV is pipe-delimited
        resp.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(resp.text), delimiter='|')
        header = next(reader, None)
        if not header:
            return []

        # Expected columns: date|speakers|title|subtitle|contents
        speeches = []
        for row in reader:
            if len(row) < 3:
                continue
            try:
                date_str = row[0].strip()
                speakers = row[1].strip() if len(row) > 1 else ''
                title = row[2].strip() if len(row) > 2 else ''
                subtitle = row[3].strip() if len(row) > 3 else ''
                contents = row[4].strip() if len(row) > 4 else ''

                # Parse date
                date = self._parse_ecb_date(date_str)
                if not date:
                    continue

                # Filter by year if specified
                if year and not date.startswith(str(year)):
                    continue

                # Generate a unique URL (ECB CSV doesn't always have URLs)
                # Use date + title hash as identifier
                url_slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:60]
                url = f"ecb://speeches/{date}/{url_slug}"

                full_title = f"{title} - {subtitle}" if subtitle else title

                speeches.append({
                    'title': full_title,
                    'date': date,
                    'url': url,
                    'speaker': speakers,
                    '_full_text': contents,  # ECB CSV includes full text
                })
            except Exception as e:
                logger.warning(f"[ECB] Error parsing CSV row: {e}")
                continue

        logger.info(f"[ECB] Parsed {len(speeches)} speeches from CSV")
        return speeches

    def _parse_ecb_date(self, date_str):
        """Parse ECB date format (YYYY-MM-DD or DD/MM/YYYY etc)."""
        from datetime import datetime

        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d %B %Y', '%Y%m%d']:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    def fetch_speech_text(self, url):
        """
        For ECB, the full text is already in the CSV.
        This method is called for edge cases where text wasn't in CSV.
        """
        # ECB speeches from CSV already have text in _full_text
        # For web-based fetching, try the ECB website
        if url.startswith('ecb://'):
            return None  # Text was already captured from CSV

        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)
        content = soup.find('div', class_='section') or soup.find('main') or soup.find('article')
        if content:
            for tag in content.find_all(['nav', 'script', 'style', 'header', 'footer']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)
        return None

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """
        Override: ECB CSV includes full text, so we handle differently.
        """
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            url = speech_info['url']
            if url in existing_urls:
                continue

            full_text = speech_info.pop('_full_text', None)
            
            # If CSV has no text, try to fetch it from the web (common for very new speeches)
            if fetch_text and (not full_text or len(full_text) < 100):
                web_text = self.fetch_speech_text(url)
                if web_text:
                    full_text = web_text

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=url,
                full_text=full_text,
            )

            if speech_id:
                new_count += 1

        logger.info(f"[ECB] Collection complete: {new_count} new speeches added")
        return new_count

    def get_all_speeches(self, start_year=None, end_year=None):
        """ECB CSV contains all speeches, no year-by-year needed."""
        speeches = self.fetch_speech_list()
        if start_year:
            speeches = [s for s in speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            speeches = [s for s in speeches if s['date'] <= f"{end_year}-12-31"]
        return speeches
