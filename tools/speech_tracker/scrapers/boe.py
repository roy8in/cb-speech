"""
Bank of England (BOE) Speech Scraper

Source: https://www.bankofengland.co.uk/news/speeches
Sitemap: https://www.bankofengland.co.uk/sitemap/speeches
"""

import re
import logging
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOEScraper(BaseScraper):
    BANK_CODE = 'BOE'
    BANK_NAME = 'Bank of England'
    BASE_URL = 'https://www.bankofengland.co.uk'

    def fetch_speech_list(self, year=None):
        """Fetch list of BOE speeches. Prefers /news/speeches for recent years."""
        current_year = datetime.now().year
        # If we are looking for the current year or recent speeches, 
        # use the news/speeches page which is much smaller than the sitemap.
        if year is None or year >= current_year - 1:
            url = f"{self.BASE_URL}/news/speeches"
        else:
            url = f"{self.BASE_URL}/sitemap/speeches"
            
        resp = self._get(url)
        if not resp:
            # Fallback
            url = f"{self.BASE_URL}/sitemap/speeches" if "news" in url else f"{self.BASE_URL}/news/speeches"
            resp = self._get(url)
            if not resp:
                return []

        soup = self._parse_html(resp.text)
        speeches = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            if not any(pattern in href for pattern in ['/speech/', '/speeches/']):
                continue
            if href == '/sitemap/speeches' or href == '/news/speeches':
                continue

            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/{href}"

            # Initial date from URL (defaults to 1st of the month)
            date = self._extract_date_from_url(href, year)

            if year and date and not date.startswith(str(year)):
                continue

            speaker = self.extract_speaker_from_title(title)

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        # Handle duplicates (e.g., HTML vs PDF for the same speech)
        # We prefer HTML over PDF
        key_map = {}
        for s in speeches:
            # Use title and date as a logical key to identify the same speech
            key = (s['title'].strip().lower(), s['date'])
            if key not in key_map:
                key_map[key] = s
            else:
                # If current is HTML and existing is PDF, replace
                is_current_pdf = s['url'].lower().endswith('.pdf')
                is_existing_pdf = key_map[key]['url'].lower().endswith('.pdf')
                if is_existing_pdf and not is_current_pdf:
                    key_map[key] = s

        return list(key_map.values())

    @staticmethod
    def extract_speaker_from_title(title):
        clean_title = re.sub(r'\(pdf\s*.*\)', '', title, flags=re.IGNORECASE).strip()
        m = re.search(r'.+[−–-]\s*(?:speech|remarks|slides|panel remarks|address)\s+by\s+([^−–-]+)$', clean_title, re.IGNORECASE)
        if m: return m.group(1).strip()
        if ':' in clean_title:
            potential = clean_title.split(':')[0].strip()
            if 1 < len(potential.split()) < 5 and not any(w in potential.lower() for w in ['at', 'the', 'meeting', 'update']):
                return potential
        m = re.search(r'Slides\s+from\s+([^’\']+)[’\']s', clean_title, re.IGNORECASE)
        if m: return m.group(1).strip()
        return None

    def _extract_date_from_url(self, href, default_year):
        # Pattern: /YYYY/Month/Slug
        match = re.search(r'/(\d{4})/(\w+)/', href)
        if match:
            year = match.group(1)
            month_str = match.group(2).title()
            # Only treat as month if it's a known month name
            from .base import BaseScraper # BaseScraper not needed, just using datetime
            for fmt in ['%B', '%b']:
                try:
                    month = datetime.strptime(month_str, fmt).month
                    return f"{year}-{month:02d}-01"
                except ValueError:
                    continue
        
        # Fallback to just year if YYYY is found
        match_year = re.search(r'/(\d{4})/', href)
        if match_year:
            return f"{match_year.group(1)}-01-01"
            
        if default_year:
            return f"{default_year}-01-01"
        return ''

    def _get_playwright(self, url):
        """Use Playwright to get page content, bypassing bot protection."""
        from playwright.sync_api import sync_playwright
        import time
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.HEADERS['User-Agent'])
            page = context.new_page()
            try:
                page.goto(url, wait_until='networkidle')
                time.sleep(2)  # Give JS time to load if necessary
                content = page.content()
                return content
            except Exception as e:
                logger.error(f"[{self.BANK_CODE}] Playwright failed for {url}: {e}")
                return None
            finally:
                browser.close()

    def fetch_speech_text(self, url):
        """Fetch the full text and exact date of a BOE speech."""
        content_type = ""
        # Check headers first to avoid downloading PDFs with playwright
        try:
            head_resp = self.session.head(url, timeout=self.REQUEST_TIMEOUT, verify=False)
            content_type = head_resp.headers.get('Content-Type', '').lower()
        except:
            pass

        if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
            resp = self._get(url)
            if resp:
                return self.extract_pdf_text(resp.content)
            return None
            
        html = self._get_playwright(url)
        
        # Fallback to standard request if Playwright fails
        if not html:
            logger.info(f"[{self.BANK_CODE}] Playwright failed, falling back to standard request for {url}")
            resp = self._get(url)
            if resp:
                html = resp.text
        
        if not html:
            return None
            
        try:
            soup = self._parse_html(html)
        except Exception as e:
            logger.warning(f"[{self.BANK_CODE}] Failed to parse HTML for {url}: {e}")
            return None
        
        # Extract precise date
        exact_date = None
        
        # 1. Check meta tags (most reliable for older pages)
        meta_date = soup.find('meta', attrs={'property': 'article:published_time'}) or \
                    soup.find('meta', attrs={'name': 'date'}) or \
                    soup.find('meta', attrs={'property': 'og:article:published_time'})
        if meta_date and meta_date.get('content'):
            try:
                # Parse ISO format like '2019-01-24T12:00:00Z'
                exact_date = meta_date['content'][:10] 
            except Exception:
                pass
                
        # 2. Check modern published-date div
        if not exact_date:
            date_el = soup.find('div', class_='published-date')
            if date_el:
                date_text = date_el.get_text(strip=True).replace('Published on', '').strip()
                try:
                    dt = datetime.strptime(date_text, '%d %B %Y')
                    exact_date = dt.strftime('%Y-%m-%d')
                except ValueError:
                    pass
                    
        # 3. Check for <time> tags
        if not exact_date:
            time_el = soup.find('time')
            if time_el and time_el.has_attr('datetime'):
                exact_date = time_el['datetime'][:10]
                
        # If exact date found, we could potentially update the DB here, 
        # but normally we return text. Let's attach date to text for the collector.
        content_el = (
            soup.find('div', class_='page-content') or
            soup.find('article') or
            soup.find('div', class_='content-block') or
            soup.find('main') or
            soup.find('body') # Ultimate fallback for very old pages
        )
        
        text = ""
        if content_el:
            # 4. Fallback: Search the first 1000 characters of text for a date
            if not exact_date:
                raw_start = content_el.get_text(separator=' ', strip=True)[:1000]
                # Look for patterns like "12 March 2019" or "March 12, 2019"
                match = re.search(r'(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})', raw_start)
                if match:
                    try:
                        dt = datetime.strptime(match.group(1), '%d %B %Y')
                        exact_date = dt.strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                        
            for tag in content_el.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside', 'button']):
                tag.decompose()
            text = content_el.get_text(separator='\n', strip=True)
            
        # Meta info hack to pass back to collector if needed
        if exact_date:
            return f"__DATE__:{exact_date}\n{text}"
        return text

    def get_all_speeches(self, start_year=None, end_year=None):
        all_speeches = self.fetch_speech_list()
        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]
        return all_speeches
