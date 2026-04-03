"""
Reserve Bank of Australia (RBA) Speech Scraper

Source: https://www.rba.gov.au/speeches/
Note: RBA blocks direct requests to year subpages (403).
      Uses Playwright to bypass bot detection.
"""

import re
import logging
import time
from datetime import datetime
from playwright.sync_api import sync_playwright
from .base import BaseScraper

logger = logging.getLogger(__name__)

class RBAScraper(BaseScraper):
    BANK_CODE = 'RBA'
    BANK_NAME = 'Reserve Bank of Australia'
    BASE_URL = 'https://www.rba.gov.au'

    def _get_playwright(self, url):
        """Use Playwright to get page content."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.HEADERS['User-Agent'])
            page = context.new_page()
            try:
                page.goto(url, wait_until='networkidle')
                time.sleep(2)
                content = page.content()
                return content
            except Exception as e:
                logger.error(f"[{self.BANK_CODE}] Playwright failed for {url}: {e}")
                return None
            finally:
                browser.close()

    def fetch_speech_list(self, year=None):
        """Fetch list of RBA speeches using Playwright."""
        # For current year, it's /speeches/. For past years, it's /speeches/YYYY/
        if year and str(year) != str(datetime.now().year):
            url = f"{self.BASE_URL}/speeches/{year}/"
        else:
            url = f"{self.BASE_URL}/speeches/"
            
        html = self._get_playwright(url)
        if not html:
            return []

        soup = self._parse_html(html)
        speeches = []

        # RBA structure: <li><a href="...">Title</a> - Speaker Name, Title (Date)</li>
        for li in soup.find_all('li'):
            link = li.find('a', href=True)
            if not link:
                continue
                
            href = link['href']
            title = link.get_text(strip=True)

            if not title or '/speeches/' not in href or not (href.endswith('.html') or href.endswith('.htm')):
                continue
            if 'index.html' in href or href == '/speeches/':
                continue

            # Full text of the li to extract speaker
            full_text = li.get_text(separator=' ', strip=True)
            
            # 1. Extract speaker name
            speaker = None
            # Pattern: Title - Speaker Name, Title (Date)
            # Find the part after the first dash and before the first comma
            m = re.search(rf"{re.escape(title)}\s*[-–—]\s*([^,]+)", full_text)
            if m:
                speaker = m.group(1).strip()
            
            # Refine RBA speaker: remove job titles if still present
            if speaker:
                for job in ['Governor', 'Deputy Governor', 'Assistant Governor', 'Senior Officer']:
                    if speaker.endswith(f" {job}"):
                        speaker = speaker.replace(f" {job}", "").strip()
                    if speaker.startswith(f"{job} "):
                        speaker = speaker.replace(f"{job} ", "").strip()

            # 2. Extract date
            # Modern pattern: YYYY-MM-DD
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', href)
            if date_match:
                date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            else:
                # Older pattern: DDMMYY (e.g., 160813)
                date_match_old = re.search(r'-(\d{2})(\d{2})(\d{2})', href)
                if date_match_old:
                    dd, mm, yy = date_match_old.groups()
                    year = f"20{yy}" if int(yy) < 50 else f"19{yy}"
                    date = f"{year}-{mm}-{dd}"
                else:
                    date = ''

            if year and date and not date.startswith(str(year)):
                continue

            # Build absolute URL
            speech_url = f"{self.BASE_URL}{href}" if href.startswith('/') else href

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        # Deduplicate
        seen = set()
        unique = []
        for s in speeches:
            if s['url'] not in seen:
                seen.add(s['url'])
                unique.append(s)

        return unique

    def fetch_speech_text(self, url):
        """Fetch the full text of an RBA speech."""
        # Handle PDFs before passing to Playwright
        if url.lower().endswith('.pdf'):
            resp = self._get(url)
            if resp:
                return self.extract_pdf_text(resp.content)
            return None
            
        html = self._get_playwright(url)
        if not html: return None
        soup = self._parse_html(html)
        
        # Detail page speaker extraction
        speaker = None
        byline = soup.find(['p', 'div'], class_=re.compile(r'byline|author|speaker'))
        if byline:
            text = byline.get_text(strip=True)
            if ',' in text:
                speaker = text.split(',')[0].strip()
        
        content = soup.find('div', id='content') or soup.find('article') or soup.find('main')
        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
                tag.decompose()
            text = content.get_text(separator='\n', strip=True)
            if speaker:
                return f"__SPEAKER__:{speaker}\n{text}"
            return text
        return None
