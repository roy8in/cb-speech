"""
Central Bank Watchtower — Unified Collector

Orchestrates all 6 scrapers, runs analysis, and sends alerts.
Designed for scheduled execution (2x daily via Task Scheduler or cron).
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.config import config
from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.scrapers import ALL_SCRAPERS

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def send_error_alert(bank_code: str, error_msg: str):
    """Sends a synchronous Telegram alert when a scraper fails."""
    if not config.TELEGRAM_API_URL or not config.ALLOWED_TELEGRAM_USER_IDS:
        return
    text = f"🚨 *Speech Tracker Error*\n\nScraper failed for: *{bank_code}*\nError: `{error_msg}`"
    try:
        for chat_id in config.ALLOWED_TELEGRAM_USER_IDS:
            requests.post(
                f"{config.TELEGRAM_API_URL}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10
            )
    except Exception as e:
        logger.error(f"Failed to send Telegram error alert: {e}")

def run_collection(banks=None, mode='recent', analyze=True, start_year=None):
    """
    Main collection pipeline.

    Args:
        banks: list of bank codes to collect, or None for all
        mode: 'recent' (current year only) or 'full' (all available years)
        analyze: whether to run NLP analysis on new speeches
        start_year: start year for full mode
    """
    db = SpeechDB()
    target_banks = banks or list(ALL_SCRAPERS.keys())

    total_new = 0
    total_refreshed = 0
    results = {}

    for bank_code in target_banks:
        if bank_code not in ALL_SCRAPERS:
            logger.warning(f"Unknown bank code: {bank_code}")
            continue

        logger.info(f"{'='*50}")
        logger.info(f"Processing: {bank_code}")
        logger.info(f"{'='*50}")

        try:
            scraper_cls = ALL_SCRAPERS[bank_code]
            scraper = scraper_cls(db=db)

            # 1. Refresh incomplete speeches (like placeholders)
            refreshed = scraper.refresh_incomplete_speeches()
            total_refreshed += refreshed
            if refreshed > 0:
                logger.info(f"[{bank_code}] Refreshed {refreshed} incomplete speeches")

            # 2. Collect new speeches
            if mode == 'full':
                new_count = scraper.collect_new_speeches(
                    start_year=start_year,
                    fetch_text=True
                )
            else:
                new_count = scraper.collect_recent(fetch_text=True)

            results[bank_code] = new_count
            total_new += new_count
            logger.info(f"[{bank_code}] {new_count} new speeches added")

        except Exception as e:
            logger.error(f"[{bank_code}] Pipeline failed: {e}")
            send_error_alert(bank_code, str(e))
            results[bank_code] = -1
            
    # 3. Apply activity-based member status cleanup globally after collection
    try:
        from scripts.speech_tracker.migrations.apply_activity_status import apply_activity_based_status
        logger.info("Running activity-based member cleanup...")
        apply_activity_based_status(days_threshold=365)
    except Exception as e:
        logger.error(f"Failed to run activity status update: {e}")

    # Run analysis on new or refreshed speeches
    if analyze and (total_new > 0 or total_refreshed > 0):
        try:
            from .analyzer import HawkDoveAnalyzer
            analyzer = HawkDoveAnalyzer(db=db)
            analyzed = analyzer.analyze_pending()
            logger.info(f"Analyzed {analyzed} speeches")
        except ImportError:
            logger.warning("Analyzer not available, skipping analysis")
        except Exception as e:
            logger.error(f"Analysis failed: {e}")

    # Print summary
    logger.info(f"\n{'='*50}")
    logger.info(f"COLLECTION SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"{'='*50}")
    for bank, count in results.items():
        status = f"{count} new" if count >= 0 else "FAILED"
        logger.info(f"  {bank}: {status}")
    logger.info(f"  Total new: {total_new}")
    logger.info(f"  Total refreshed: {total_refreshed}")

    # Print overall stats
    stats = db.get_stats()
    logger.info(f"\nDatabase totals:")
    for bank in sorted(stats.keys()):
        if bank == 'total':
            continue
        s = stats[bank]
        logger.info(f"  {bank}: {s['total_speeches']} speeches ({s['analyzed']} analyzed)")
    logger.info(f"  Grand total: {stats['total']} speeches")

    return results


def main():
    parser = argparse.ArgumentParser(description='Central Bank Speech Collector')
    parser.add_argument('--banks', nargs='+',
                        choices=list(ALL_SCRAPERS.keys()),
                        help='Specific banks to collect (default: all)')
    parser.add_argument('--mode', choices=['recent', 'full'], default='recent',
                        help='recent=current year only, full=all available years')
    parser.add_argument('--start-year', type=int, default=None,
                        help='Start year for full mode (default: earliest available)')
    parser.add_argument('--no-analyze', action='store_true',
                        help='Skip NLP analysis')
    parser.add_argument('--stats', action='store_true',
                        help='Show database stats and exit')
    parser.add_argument('--test', action='store_true',
                        help='Test mode: fetch 1 speech from each bank')

    args = parser.parse_args()

    if args.stats:
        db = SpeechDB()
        stats = db.get_stats()
        print(f"\n{'='*40}")
        print(f"Central Bank Watchtower — Database Stats")
        print(f"{'='*40}")
        for bank in sorted(stats.keys()):
            if bank == 'total':
                continue
            s = stats[bank]
            print(f"  {bank}: {s['total_speeches']} speeches ({s['analyzed']} analyzed)")
        print(f"  Total: {stats['total']} speeches")
        return

    if args.test:
        print("Running test mode...")
        db = SpeechDB()
        for bank_code, scraper_cls in ALL_SCRAPERS.items():
            try:
                scraper = scraper_cls(db=db)
                speeches = scraper.fetch_speech_list()
                if speeches:
                    print(f"\n[{bank_code}] Found {len(speeches)} speeches. First:")
                    s = speeches[0]
                    print(f"  Title: {s['title'][:80]}")
                    print(f"  Date:  {s['date']}")
                    print(f"  URL:   {s['url'][:80]}")
                    print(f"  Speaker: {s.get('speaker', 'N/A')}")
                else:
                    print(f"\n[{bank_code}] No speeches found")
            except Exception as e:
                print(f"\n[{bank_code}] ERROR: {e}")
        return

    run_collection(
        banks=args.banks,
        mode=args.mode,
        analyze=not args.no_analyze,
        start_year=args.start_year,
    )


if __name__ == '__main__':
    main()
