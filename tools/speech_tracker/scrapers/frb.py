"""
Federal Reserve Board (FRB) Speech Scraper

Source: https://www.federalreserve.gov/newsevents/speeches.htm
Year index (2011+): https://www.federalreserve.gov/newsevents/{YYYY}-speeches.htm
Year index (≤2010): https://www.federalreserve.gov/newsevents/{YYYY}speech.htm
Individual: https://www.federalreserve.gov/newsevents/speech/{speaker}{YYYYMMDD}{seq}.htm
"""

import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class FRBScraper(BaseScraper):
    BANK_CODE = 'FRB'
    BANK_NAME = 'Federal Reserve'
    BASE_URL = 'https://www.federalreserve.gov'
    
    def _get_year_url(self, year):
        """Get the correct URL for a given year's speech list."""
        if year >= 2011:
            return f"{self.BASE_URL}/newsevents/{year}-speeches.htm"
        else:
            return f"{self.BASE_URL}/newsevents/{year}speech.htm"

    def _lookup_speaker(self, last_name):
        """Dynamic speaker lookup in the DB based on last name."""
        if not last_name:
            return None
            
        conn = self.db._get_conn()
        try:
            # Look for active members in FRB whose name ends with the provided last name
            # or matches common patterns.
            row = conn.execute("""
                SELECT name FROM members 
                WHERE bank_code = 'FRB' 
                AND (name LIKE ? OR name LIKE ?)
                ORDER BY status='active' DESC, last_speech_date DESC
                LIMIT 1
            """, (f"% {last_name}", f"{last_name}%")).fetchone()
            
            return row['name'] if row else last_name
        finally:
            conn.close()

    def fetch_speech_list(self, year=None):
        """Fetch list of Fed speeches for a given year."""
        if year is None:
            from datetime import datetime
            year = datetime.now().year

        url = self._get_year_url(year)
        resp = self._get(url)
        if not resp:
            return []

        soup = self._parse_html(resp.text)
        speeches = []

        # Find all links that point to speech pages (search entire document)
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            # Filter: only speech URLs with sufficient title length
            if '/newsevents/speech/' not in href:
                continue
            if not title or len(title) < 10:
                continue
            # Skip navigation links
            if title.lower() in ('speech', 'speeches', 'archive', 'more'):
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/newsevents/speech/{href}"

            # Extract date from URL: {speaker}{YYYYMMDD}{seq}.htm
            date_match = re.search(r'(\d{8})', href)
            date = ''
            if date_match:
                d = date_match.group(1)
                date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            else:
                date = f"{year}-01-01"

            # Extract speaker from URL or title
            speaker = None
            speaker_match = re.search(r'/speech/([a-z]+)\d{8}', href)
            if speaker_match:
                speaker_slug = speaker_match.group(1).title()
                speaker = self._lookup_speaker(speaker_slug)

            # Try to extract from surrounding text (date/speaker in parent)
            parent = link.parent
            if parent and not speaker:
                parent_text = parent.get_text()
                for pattern in [
                    r'(?:Governor|Chair|Vice Chair)\s+(\w+(?:\s+\w+)?)',
                ]:
                    m = re.search(pattern, parent_text)
                    if m:
                        speaker = m.group(1)
                        break

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        # Deduplicate by URL
        seen = set()
        unique = []
        for s in speeches:
            if s['url'] not in seen:
                seen.add(s['url'])
                unique.append(s)

        return unique

    def fetch_speech_text(self, url):
        """Fetch the full text of a Fed speech."""
        resp = self._get(url)
        if not resp:
            return None

        content_type = resp.headers.get('Content-Type', '').lower()
        if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
            return self.extract_pdf_text(resp.content)

        try:
            soup = self._parse_html(resp.text)
        except Exception as e:
            logger.warning(f"[{self.BANK_CODE}] Failed to parse HTML for {url}: {e}")
            return None

        # Try various content selectors
        content = (
            soup.find('div', class_='col-xs-12 col-sm-8 col-md-8') or
            soup.find('div', id='article') or
            soup.find('div', class_='article') or
            soup.find('main') or
            soup.find('article')
        )

        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """Fetch all speeches from 2006 onwards."""
        return super().get_all_speeches(
            start_year=start_year or 2006,
            end_year=end_year,
        )
