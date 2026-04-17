from bs4 import BeautifulSoup
import re

print("ECB:")
with open('ecb_debug.html') as f:
    soup = BeautifulSoup(f, 'html.parser')
    for dl in soup.find_all(['dl', 'ul', 'div'], limit=5):
        # The ECB scraper looks for <dt> and <dd>. Let's see if they changed to <article> or something.
        print(f"Tag: {dl.name}, Class: {dl.get('class')}")
    # Let's find first link that might be a speech
    for a in soup.find_all('a', href=re.compile(r'/press/key/date/')):
        print("Speech Link:", a.parent.parent.prettify()[:500])
        break
    # Or just search for the title of a recent speech
    print("Recent items with 'speech':")
    for a in soup.find_all('a', string=re.compile(r'speech|remarks|presentation', re.I)):
        print(a.parent.parent.prettify()[:500])
        break

print("\nRBA:")
with open('rba_debug.html') as f:
    soup = BeautifulSoup(f, 'html.parser')
    for a in soup.find_all('a', href=re.compile(r'/speeches/')):
        if not a.get_text(strip=True): continue
        print("RBA link context:", a.parent.prettify()[:500])
        break

print("\nBOE:")
with open('boe_debug.html') as f:
    soup = BeautifulSoup(f, 'html.parser')
    for a in soup.find_all('a', href=re.compile(r'/speech/|/speeches/')):
        print("BOE link context:", a.parent.prettify()[:500])
        break
