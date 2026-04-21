from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.scrapers.boe import BOEScraper
import logging

logging.basicConfig(level=logging.INFO)
boe = BOEScraper()
html = boe._get_playwright("https://www.bankofengland.co.uk/news/speeches", wait_ms=5000)
with open('boe_playwright.html', 'w') as f:
    f.write(html or "None")
print("Saved BOE playwright HTML")
