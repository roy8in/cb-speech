import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://www.ecb.europa.eu/rss/press.html'
r = requests.get(url)
soup = BeautifulSoup(r.content, 'xml')

speeches = []
for item in soup.find_all('item'):
    link = item.find('link').get_text(strip=True)
    if '/press/key/' in link or '/press/inter/' in link:
        title = item.find('title').get_text(strip=True)
        pub_date = item.find('pubDate').get_text(strip=True)
        # Parse pub_date like 'Thu, 16 Apr 2026 15:15:00 +0200'
        dt = datetime.strptime(pub_date[:-6].strip(), '%a, %d %b %Y %H:%M:%S')
        date = dt.strftime('%Y-%m-%d')
        speeches.append({'title': title, 'date': date, 'url': link})

print(f"Found {len(speeches)} speeches/interviews in RSS")
for s in speeches[:3]:
    print(s)
