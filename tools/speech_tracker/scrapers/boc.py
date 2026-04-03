"""
Bank of Canada (BOC) Speech Scraper

Source: https://www.bankofcanada.ca/press/speeches/
List page uses pagination via ?mt_page=N
Speech URLs: /YYYY/MM/slug/
Multimedia (webcasts) URLs: /multimedia/slug/ (excluded)
"""

import re
import logging
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOCScraper(BaseScraper):
    BANK_CODE = 'BOC'
    BANK_NAME = 'Bank of Canada'
    BASE_URL = 'https://www.bankofcanada.ca'
    SPEECHES_URL = f'{BASE_URL}/press/speeches/'

    def fetch_speech_list(self, year=None):
        """
        Fetch list of BOC speeches with pagination support.
        If year is given, filter results to that year.
        """
        all_speeches = []
        page = 1
        max_pages = 300  # Increased limit to allow fetching back to 2008

        while page <= max_pages:
            url = self.SPEECHES_URL
            if page > 1:
                url = f"{self.SPEECHES_URL}?mt_page={page}"

            resp = self._get(url)
            if not resp:
                break

            soup = self._parse_html(resp.text)
            speeches_on_page = self._parse_speech_list_page(soup)

            if not speeches_on_page:
                break  # no more results

            all_speeches.extend(speeches_on_page)
            page += 1

            # Check if there's a next page
            if not self._has_next_page(soup):
                break

        # Filter by year if specified
        if year:
            year_str = str(year)
            all_speeches = [s for s in all_speeches if s['date'].startswith(year_str)]

        # Deduplicate by URL
        unique = {s['url']: s for s in all_speeches}
        logger.info(f"[{self.BANK_CODE}] Found {len(unique)} speeches")
        return list(unique.values())

    def _parse_speech_list_page(self, soup):
        """Parse a single page of the speech list."""
        speeches = []

        # Each speech entry is typically inside an <article> or similar container
        # The date is in a span with class 'media-date'
        for container in soup.find_all(['div', 'article'], class_=['media', 'mtt-result']):
            h3 = container.find(['h3', 'h5'])
            if not h3:
                continue

            link = h3.find('a', href=True)
            if not link:
                continue

            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            # Skip multimedia/webcast links - only collect text speeches
            if '/multimedia/' in href:
                continue

            # Only collect links matching /YYYY/MM/ pattern (actual speech pages)
            if not re.search(r'/\d{4}/\d{2}/', href):
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                continue

            # 1. Extract date from text (e.g., "March 4, 2026")
            date = ''
            date_tag = container.find('span', class_=['media-date', 'pressdate'])
            if date_tag:
                date_text = date_tag.get_text(strip=True)
                date = self._parse_boc_date(date_text)

            # 2. Fallback to URL date if text date extraction failed
            if not date:
                date_match = re.search(r'/(\d{4})/(\d{2})/', href)
                if date_match:
                    date = f"{date_match.group(1)}-{date_match.group(2)}-01"

            # 3. Extract speaker from nearby /profile/ link
            speaker = self._extract_speaker(h3)
            
            # 4. Fallback: Extract from title if speaker is still None
            if not speaker:
                for sep in [':', '—', ' - ']:
                    if sep in title:
                        potential = title.split(sep)[0].strip()
                        if 1 < len(potential.split()) < 5:
                            speaker = potential
                            break

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        return speeches

    def _parse_boc_date(self, date_text):
        """Parse BOC date formats like 'March 4, 2026' or 'March 04, 2026'."""
        if not date_text:
            return None
            
        # Clean the text (remove extra spaces, non-breaking spaces)
        date_text = date_text.replace('\xa0', ' ').strip()
        
        for fmt in ['%B %d, %Y', '%b %d, %Y', '%Y-%m-%d']:
            try:
                dt = datetime.strptime(date_text, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
                
        # Try a more flexible regex for dates like "March 4, 2026"
        match = re.search(r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})', date_text)
        if match:
            month_str, day_str, year_str = match.groups()
            try:
                # Use strptime with %B for full month name
                dt = datetime.strptime(f"{month_str} {day_str} {year_str}", "%B %d %Y")
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        return None

    def _extract_speaker(self, h3_tag):
        """Extract speaker name from the /profile/ link near the h3 tag."""
        # Look in the parent container for a profile link
        parent = h3_tag.parent
        if not parent:
            return None

        # Search up to 2 levels of parents/siblings
        container = h3_tag.find_parent(['div', 'article'])
        if container:
            profile_link = container.find('a', href=re.compile(r'/profile/'))
            if profile_link:
                # Remove extra info like 'Governor - Executive' if needed
                return profile_link.get_text(strip=True)

        return None

    def _has_next_page(self, soup):
        """Check if there's a next page in pagination."""
        pagination = soup.find('a', class_='next') or soup.find('a', string=re.compile(r'Next|›|»'))
        if pagination:
            return True
        return False

    def fetch_speech_text(self, url):
        """Fetch the full text of a BOC speech."""
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

        # Try the most logical main containers
        content = soup.find('main') or soup.find('article') or soup.find('div', id='main-content')
        
        if content:
            # Aggressively remove known navigation and redundant sections
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside', 'form']):
                tag.decompose()
            
            # BOC specific: remove modules that are typically sidebar or navigation
            for tag in content.find_all('div', class_=['related-info', 'media-sidebar', 'sharing-tools', 'cfct-sidebar']):
                tag.decompose()

            text = content.get_text(separator='\n', strip=True)
            
            # If the text starts with "About us" or "What we do", it's likely still grabbing the menu
            # Let's try to find the first <h2> or <h3> which is usually the title/introduction
            if "About us" in text[:200]:
                real_start = content.find(['h1', 'h2', 'h3'])
                if real_start:
                    # Collect text only from the real start onwards
                    all_text = []
                    for sibling in real_start.find_all_next(string=True):
                        all_text.append(sibling)
                    text = '\n'.join(all_text).strip()

            return text

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """
        BOC uses pagination, not year-based URLs.
        Fetch all and filter by year range.
        """
        all_speeches = self.fetch_speech_list()

        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]

        return all_speeches
