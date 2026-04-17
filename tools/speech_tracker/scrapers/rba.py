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
from .base import BaseScraper

logger = logging.getLogger(__name__)

class RBAScraper(BaseScraper):
    BANK_CODE = 'RBA'
    BANK_NAME = 'Reserve Bank of Australia'
    BASE_URL = 'https://www.rba.gov.au'

    def fetch_speech_list(self, year=None):
        """Fetch list of RBA speeches using Playwright."""
        # For current year, it's /speeches/. For past years, it's /speeches/YYYY/
        if year and str(year) != str(datetime.now().year):
            url = f"{self.BASE_URL}/speeches/{year}/"
        else:
            url = f"{self.BASE_URL}/speeches/"
            
        html = self._get_playwright(url, wait_ms=3000)
        if not html:
            return []

        soup = self._parse_html(html)
        speeches = []

        # RBA structure: modern pages use <article class="item">, older ones might just be links
        for article in soup.find_all(['article', 'div', 'li']):
            link = article.find('a', href=True)
            if not link: continue
            
            href = link['href']
            title = link.get_text(strip=True)

            if not title or '/speeches/' not in href or not (href.endswith('.html') or href.endswith('.htm')):
                continue
            if 'index.html' in href or href == '/speeches/' or '/speeches/list.html' in href:
                continue
            
            # Skip common non-speech links that might be in the same area
            if title.lower() in ('audio', 'transcript', 'q&a transcript', 'q&a', 'video', 'presentation'):
                continue

            full_entry_text = article.get_text(separator=' ', strip=True)
            
            # 1. Extract speaker name
            speaker = None
            author_tag = article.find(class_=re.compile(r'author-name|speaker'))
            if author_tag:
                speaker = author_tag.get_text(strip=True)
            else:
                # Pattern: Title - Speaker Name, Title (Date)
                # Find the part after the first dash and before the first comma
                m = re.search(rf"{re.escape(title)}\s*[-–—]\s*([^,]+)", full_entry_text)
                if m:
                    speaker = m.group(1).strip()
            
            # 2. Extract date
            date = ''
            time_tag = article.find('time')
            if time_tag and time_tag.get('datetime'):
                date = time_tag['datetime'][:10]
            else:
                # Modern pattern: YYYY-MM-DD in URL
                date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', href)
                if date_match:
                    date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                else:
                    # Older pattern: DDMMYY (e.g., 160813)
                    date_match_old = re.search(r'-(\d{2})(\d{2})(\d{2})', href)
                    if date_match_old:
                        dd, mm, yy = date_match_old.groups()
                        y = f"20{yy}" if int(yy) < 50 else f"19{yy}"
                        date = f"{y}-{mm}-{dd}"
                    else:
                        # Look for date in text: "16 April 2026"
                        date_text_match = re.search(r'(\d{1,2})\s+([A-Z][a-z]+)\s+(\d{4})', full_entry_text)
                        if date_text_match:
                            try:
                                dt = datetime.strptime(date_text_match.group(0), '%d %B %Y')
                                date = dt.strftime('%Y-%m-%d')
                            except ValueError:
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

        # Sort by date descending
        unique.sort(key=lambda x: x['date'], reverse=True)

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
        
        # 1. Look for byline/author meta
        byline = soup.find(['p', 'div'], class_=re.compile(r'byline|author|speaker'))
        if byline:
            text = byline.get_text(strip=True)
            if ',' in text:
                speaker = text.split(',')[0].strip()
        
        content_div = soup.find('div', id='content') or soup.find('article') or soup.find('main')
        if content_div:
            # Clean up content
            for tag in content_div.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
                tag.decompose()
            text = content_div.get_text(separator='\n', strip=True)
            
            # 2. If speaker still missing, try extracting from the first few lines of text
            if not speaker:
                lines = [l.strip() for l in text.split('\n') if l.strip()][:5]
                if lines:
                    # Pattern for Q&A: Transcript... / [Title] / [Speaker Name]
                    if "Transcript" in lines[0] and len(lines) >= 3:
                        for idx in [2, 3]:
                            if idx < len(lines) and 2 <= len(lines[idx].split()) <= 4:
                                if re.match(r'^[A-Za-z\s\.]+$', lines[idx]):
                                    speaker = lines[idx]
                                    break
                    # Pattern for normal speech: [Title] / [Speaker Name]
                    elif len(lines) >= 2:
                        for idx in [1, 2]:
                            if idx < len(lines) and 2 <= len(lines[idx].split()) <= 4:
                                if re.match(r'^[A-Za-z\s\.]+$', lines[idx]):
                                    if lines[idx].lower() not in ['governor', 'deputy governor', 'assistant governor', 'senior officer', 'audio', 'transcript', 'speech']:
                                        speaker = lines[idx]
                                        break
            
            if speaker:
                # Clean speaker: remove job titles
                for job in ['Governor', 'Deputy Governor', 'Assistant Governor', 'Senior Officer']:
                    if speaker.endswith(f" {job}"):
                        speaker = speaker.replace(f" {job}", "").strip()
                    if speaker.startswith(f"{job} "):
                        speaker = speaker.replace(f"{job} ", "").strip()
                
                speaker = " ".join(speaker.split())
                return f"__SPEAKER__:{speaker}\n{text}"
            return text
        return None
