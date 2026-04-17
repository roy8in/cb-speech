from tools.speech_tracker.scrapers.ecb import ECBScraper
from tools.speech_tracker.scrapers.rba import RBAScraper
from tools.speech_tracker.scrapers.boe import BOEScraper

import json

ecb = ECBScraper()
html_ecb = ecb._get_playwright(f"{ecb.BASE_URL}/press/key/html/index.en.html")
with open('ecb_debug.html', 'w') as f: f.write(html_ecb or "None")

rba = RBAScraper()
html_rba = rba._get_playwright(f"{rba.BASE_URL}/speeches/")
with open('rba_debug.html', 'w') as f: f.write(html_rba or "None")

boe = BOEScraper()
html_boe = boe._get("https://www.bankofengland.co.uk/news/speeches")
with open('boe_debug.html', 'w') as f: f.write(html_boe.text if html_boe else "None")
