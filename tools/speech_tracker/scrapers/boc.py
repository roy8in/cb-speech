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
    ALLOWED_CONTENT_TYPES = {
        'Comments',
        'Lectures',
        'Opening statements',
        'Presentations',
        'Remarks',
        'Speech summaries',
    }

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

        # Restrict parsing to the page's main content. The full BOC HTML also
        # contains menu/news teaser blocks with the same "media" class.
        root = soup.find('main') or soup

        # Each speech entry is typically inside a .media result container.
        # The date is in a span with class 'media-date'.
        for container in root.find_all(['div', 'article'], class_=['media', 'mtt-result']):
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

            content_types = self._extract_content_types(container)
            if not self._is_collectable_content_type(content_types):
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
                'speech_type': self._primary_speech_type(content_types),
            })

        return speeches

    def _extract_content_types(self, container):
        """Return BOC content-type labels attached to a result card."""
        content_types = []
        for link in container.find_all('a'):
            label = link.get_text(" ", strip=True)
            if label in self.ALLOWED_CONTENT_TYPES or label == 'Webcasts':
                content_types.append(label)
        return content_types

    def _is_collectable_content_type(self, content_types):
        """Collect textual speech-like BOC content, not webcasts/news items."""
        if not content_types:
            return False
        if 'Webcasts' in content_types:
            return False
        return any(ct in self.ALLOWED_CONTENT_TYPES for ct in content_types)

    def _primary_speech_type(self, content_types):
        for content_type in content_types:
            if content_type in self.ALLOWED_CONTENT_TYPES:
                return content_type
        return 'speech'

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

        if self._is_pdf_response(url, resp):
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
            text = self._clean_speech_text(text)
            
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

    def collect_recent(self, fetch_text=True):
        """Collect current-year BOC speeches and refresh changed existing rows."""
        current_year = datetime.now().year
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        norm_existing_urls = {self.normalize_url(u) for u in existing_urls}

        speech_list = self.fetch_speech_list(year=current_year)
        if not speech_list:
            return 0

        new_count = 0
        refreshed_count = 0
        for speech_info in speech_list:
            url = self.normalize_url(speech_info['url'])
            if url in norm_existing_urls:
                if self._refresh_existing_speech(speech_info, fetch_text=fetch_text):
                    refreshed_count += 1
                continue

            if self.is_logical_duplicate(self.BANK_CODE, speech_info['title'], speech_info['date']):
                continue

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(speech_info['url'])

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=speech_info['url'],
                full_text=full_text,
                speech_type=speech_info.get('speech_type', 'speech'),
            )
            if speech_id:
                new_count += 1

        if refreshed_count:
            logger.info(f"[{self.BANK_CODE}] Refreshed {refreshed_count} existing recent speeches")
        return new_count

    def _refresh_existing_speech(self, speech_info, fetch_text=True):
        """Refresh metadata/text for an existing BOC URL when the page changed."""
        conn = self.db._get_conn()
        try:
            row = conn.execute(
                """
                SELECT s.id, s.title, s.date, s.speech_type, s.full_text, m.name AS speaker
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                WHERE s.url = ?
                """,
                (speech_info['url'],),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            return False

        desired_type = speech_info.get('speech_type', 'speech')
        dirty_text = self._looks_like_dirty_boc_text(row['full_text'])
        metadata_changed = (
            row['title'] != speech_info['title']
            or row['date'] != speech_info['date']
            or row['speech_type'] != desired_type
            or row['speaker'] != speech_info.get('speaker')
        )

        if not metadata_changed and not dirty_text:
            return False

        full_text = row['full_text']
        if fetch_text and (dirty_text or metadata_changed):
            full_text = self.fetch_speech_text(speech_info['url']) or full_text

        speaker_id = self.db.get_or_create_member(self.BANK_CODE, speech_info.get('speaker'))
        conn = self.db._get_conn()
        try:
            conn.execute(
                """
                UPDATE speeches
                SET title = ?,
                    date = ?,
                    speaker_id = ?,
                    full_text = ?,
                    speech_type = ?,
                    fetched_at = ?,
                    synced_at = NULL
                WHERE id = ?
                """,
                (
                    speech_info['title'],
                    speech_info['date'],
                    speaker_id,
                    full_text,
                    desired_type,
                    datetime.now().isoformat(),
                    row['id'],
                ),
            )
            conn.commit()
        finally:
            conn.close()

        return True

    def _looks_like_dirty_boc_text(self, text):
        if not text:
            return False
        return any(
            marker in text
            for marker in ('Share this page', 'Content Type(s)', 'Related Information')
        )

    def _clean_speech_text(self, text):
        """Remove BOC page chrome while keeping title, metadata, and remarks."""
        if not text:
            return text

        lines = []
        previous = None
        for raw_line in text.replace('\xa0', ' ').splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line == previous:
                continue
            if line.startswith('Share this page'):
                continue
            if line in {'Available as:', 'PDF', 'Audio', 'Video'}:
                continue
            lines.append(line)
            previous = line

        cut_markers = {'Content Type(s)', 'Subject(s)', 'Related Information'}
        for index, line in enumerate(lines):
            if line in cut_markers:
                lines = lines[:index]
                break

        return '\n'.join(lines).strip()

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
