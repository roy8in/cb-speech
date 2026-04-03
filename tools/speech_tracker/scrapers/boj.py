"""
Bank of Japan (BOJ) Speech Scraper

Source: https://www.boj.or.jp/en/about/press/koen_year/index.htm
"""

import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOJScraper(BaseScraper):
    BANK_CODE = 'BOJ'
    BANK_NAME = 'Bank of Japan'
    BASE_URL = 'https://www.boj.or.jp'

    def fetch_speech_list(self, year=None):
        """Fetch list of BOJ speeches for a given year."""
        year_str = str(year) if year else "2026"
        url = f"{self.BASE_URL}/en/about/press/koen_{year_str}/index.htm"
        
        resp = self._get(url)
        if not resp:
            # Older archives might be under a different path or not exist, but we still try
            return []

        soup = self._parse_html(resp.text)
        speeches = []

        # Broaden search: look at all list items and table cells
        for container in soup.find_all(['li', 'td']):
            link = container.find('a', href=True)
            if not link:
                continue

            title = link.get_text(strip=True)
            href = link['href']

            if not title or len(title) < 5:
                continue

            # BOJ speech links usually start with /en/about/press/koen_YYYY/
            # but older ones might be slightly different. Look for 'koen' or 'press' + pdf/htm
            if '/koen_' not in href and 'koen' not in href:
                continue

            # Skip index/navigation links
            if href.endswith('index.htm') or 'r_menu' in href:
                continue

            # Build absolute URL
            speech_url = f"{self.BASE_URL}{href}" if href.startswith('/') else href

            # Extract date: in modern <li> it's in the li text. In older <td>, it might be in the previous sibling <td> or same td
            date_text = container.get_text(strip=True)
            date = self._parse_boj_date(date_text)
            
            if not date and container.name == 'td':
                # Check previous sibling for date in table layouts
                prev = container.find_previous_sibling('td')
                if prev:
                    date = self._parse_boj_date(prev.get_text(strip=True))
            
            # Extract speaker from title parentheses if present
            speaker = None
            # Pattern: (Speech by Governor UEDA Kazuo)
            m = re.search(r'\((?:Speech|Remarks|Address)\s+by\s+(?:Governor\s+|Deputy Governor\s+)?([^)]+)\)', title, re.IGNORECASE)
            if m:
                speaker = m.group(1).strip()

            speeches.append({
                'title': title,
                'date': date or f"{year_str}-01-01",
                'url': speech_url,
                'speaker': speaker,
            })

        # Handle Full Text vs Summary
        key_map = {}
        for s in speeches:
            # Normalize title for key: remove " (Summary)" or " (Full Text)"
            clean_title = re.sub(r'\s*\((?:Summary|Outline|Full Text)\)', '', s['title'], flags=re.IGNORECASE).strip().lower()
            key = (clean_title, s['date'])
            
            if key not in key_map:
                key_map[key] = s
            else:
                # Prefer "Full Text" in title or HTML over PDF/Summary
                is_current_full = 'full text' in s['title'].lower() or not s['url'].lower().endswith('.pdf')
                is_existing_full = 'full text' in key_map[key]['title'].lower() or not key_map[key]['url'].lower().endswith('.pdf')
                if not is_existing_full and is_current_full:
                    key_map[key] = s
        
        return list(key_map.values())

    def _parse_boj_date(self, text):
        """Parse BOJ date format (e.g., 'Mar. 3, 2026')."""
        from datetime import datetime
        # Regex to find something like "Mar. 3, 2026"
        match = re.search(r'([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})', text)
        if match:
            date_str = match.group(1).replace('.', '')
            for fmt in ['%b %d, %Y', '%B %d, %Y']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        return None

    def fetch_speech_text(self, url):
        """Fetch the full text and extract speaker from the body."""
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

        content = soup.find('div', id='main') or soup.find('div', class_='section') or soup.find('main')
        
        if content:
            # 1. Extract speaker from the first few paragraphs
            # Pattern: UEDA Kazuo Governor of the Bank of Japan
            speaker = None
            first_p = content.find('p')
            if first_p:
                p_text = first_p.get_text(strip=True)
                # Look for name before "Governor" or "Deputy Governor"
                m = re.search(r'^([^,]+?)\s+(?:Governor|Deputy Governor)', p_text)
                if m:
                    speaker = m.group(1).strip()
            
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
                tag.decompose()
            
            text = content.get_text(separator='\n', strip=True)
            if speaker:
                return f"__SPEAKER__:{speaker}\n{text}"
            return text
            
        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        from datetime import datetime
        current_year = datetime.now().year
        start = start_year or 2019
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):
            speeches = self.fetch_speech_list(year=year)
            if speeches:
                all_speeches.extend(speeches)
        return all_speeches
