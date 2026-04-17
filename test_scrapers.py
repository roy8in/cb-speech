import sys
import logging
logging.basicConfig(level=logging.INFO)

from tools.speech_tracker.scrapers.ecb import ECBScraper
from tools.speech_tracker.scrapers.rba import RBAScraper
from tools.speech_tracker.scrapers.boe import BOEScraper

try:
    ecb = ECBScraper()
    print("ECB SPEECHES:")
    ecb_speeches = ecb.fetch_speech_list(year=2026)
    print(f"Found {len(ecb_speeches)} ECB speeches for 2026")
    for s in ecb_speeches[:3]: print(s)
except Exception as e:
    print(f"ECB Error: {e}")

try:
    rba = RBAScraper()
    print("\nRBA SPEECHES:")
    rba_speeches = rba.fetch_speech_list(year=2026)
    print(f"Found {len(rba_speeches)} RBA speeches for 2026")
    for s in rba_speeches[:3]: print(s)
except Exception as e:
    print(f"RBA Error: {e}")

try:
    boe = BOEScraper()
    print("\nBOE SPEECHES:")
    boe_speeches = boe.fetch_speech_list(year=2026)
    print(f"Found {len(boe_speeches)} BOE speeches for 2026")
    for s in boe_speeches[:3]: print(s)
except Exception as e:
    print(f"BOE Error: {e}")
