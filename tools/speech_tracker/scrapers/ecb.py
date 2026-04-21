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
        speeches = self._fetch_from_csv(year)
        
        # If looking for recent speeches, also check the HTML index
        from datetime import datetime
        current_year = datetime.now().year
        if year is None or year >= current_year - 1:
            recent_html = self.fetch_recent_from_html()
            # Combine and deduplicate by URL/Logical key
            existing_urls = {s['url'] for s in speeches}
            for s in recent_html:
                if s['url'] not in existing_urls:
                    speeches.append(s)
                    existing_urls.add(s['url'])
        
        return speeches

    def fetch_recent_speeches(self):
        """Fetch only the latest ECB speeches from the RSS feed."""
        return self.fetch_recent_from_html()

    def _fetch_from_csv(self, year=None):
        resp = self._get(self.CSV_URL)
        if not resp:
            return []

        # ECB CSV is pipe-delimited
        resp.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(resp.text), delimiter='|')
        header = next(reader, None)
        if not header:
            return []

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

                date = self._parse_ecb_date(date_str)
                if not date: continue
                if year and not date.startswith(str(year)): continue

                # Generate a unique URL for CSV entries
                url_slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:60]
                url = f"ecb://speeches/{date}/{url_slug}"
                full_title = f"{title} - {subtitle}" if subtitle else title

                speeches.append({
                    'title': full_title,
                    'date': date,
                    'url': url,
                    'speaker': speakers,
                    '_full_text': contents,
                })
            except Exception as e:
                logger.warning(f"[ECB] Error parsing CSV row: {e}")
                continue
        return speeches

    def fetch_recent_from_html(self):
        """Fetch the most recent speeches from the ECB's RSS feed."""
        url = "https://www.ecb.europa.eu/rss/press.html"
        resp = self._get(url)
        if not resp:
            return []

        speeches = []
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            
            from datetime import datetime
            for item in root.findall('.//item'):
                link_node = item.find('link')
                if link_node is None or not link_node.text: continue
                href = link_node.text.strip()
                
                # ECB speeches and interviews
                if '/press/key/' not in href and '/press/inter/' not in href:
                    continue

                title_node = item.find('title')
                title = title_node.text.strip() if title_node is not None and title_node.text else ""
                
                pub_date_node = item.find('pubDate')
                date = ""
                if pub_date_node is not None and pub_date_node.text:
                    pub_date = pub_date_node.text.strip()
                    try:
                        # Format: 'Thu, 16 Apr 2026 15:15:00 +0200'
                        dt = datetime.strptime(pub_date[:-6].strip(), '%a, %d %b %Y %H:%M:%S')
                        date = dt.strftime('%Y-%m-%d')
                    except ValueError:
                        pass

                # Try to extract speaker from title
                speaker = ""
                if "Speech by" in title:
                    speaker = title.split("Speech by")[-1].split(",")[0].strip()
                elif "Interview with" in title:
                    speaker = title.split("Interview with")[-1].split(",")[0].strip()

                speeches.append({
                    'title': title,
                    'date': date,
                    'url': href,
                    'speaker': speaker,
                })
        except Exception as e:
            logger.error(f"[ECB] Error parsing RSS feed: {e}")

        logger.info(f"[ECB] Found {len(speeches)} speeches from RSS feed")
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

        # Check for PDF (often linked directly from RSS)
        if url.lower().endswith('.pdf'):
            resp = self._get(url)
            if resp:
                return self.extract_pdf_text(resp.content)
            return None

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

    def collect_recent(self, fetch_text=True):
        """
        Optimized recent collection path.

        Uses RSS only instead of downloading the full CSV dataset on every run.
        """
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.fetch_recent_speeches()

        new_count = 0
        for speech_info in speech_list:
            url = speech_info['url']
            if url in existing_urls:
                continue

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(url)

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

        logger.info(f"[ECB] Recent collection complete: {new_count} new speeches added")
        return new_count

    def get_all_speeches(self, start_year=None, end_year=None):
        """ECB CSV contains all speeches, no year-by-year needed."""
        speeches = self.fetch_speech_list()
        if start_year:
            speeches = [s for s in speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            speeches = [s for s in speeches if s['date'] <= f"{end_year}-12-31"]
        return speeches
