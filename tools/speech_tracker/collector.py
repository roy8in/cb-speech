"""Unified collector for central-bank speeches.

The collector writes only to the local SQLite database. Remote warehouse
loading is intentionally outside this module.
"""

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ops_status import (
    append_event,
    next_daily_eastern_run,
    update_bank,
    update_stage,
    update_status,
)
from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.pipeline_log import log_pipeline_job
from tools.speech_tracker.scrapers import ALL_SCRAPERS


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def utc_now_iso():
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def run_collection(
    banks=None,
    mode="recent",
    analyze=True,
    start_year=None,
    run_id=None,
    pipeline_logger=None,
):
    """Collect speeches, refresh incomplete text, and run initial analysis."""
    db = SpeechDB()
    run_id = run_id or uuid.uuid4().hex
    target_banks = banks or list(ALL_SCRAPERS.keys())
    next_run_at = next_daily_eastern_run()
    started_at = utc_now_iso()

    append_event(
        {
            "service": "cb-speeches",
            "stage": "collection",
            "status": "started",
            "run_id": run_id,
            "message": f"collection started for {','.join(target_banks)}",
        }
    )
    update_status(
        run_id=run_id,
        state="running",
        next_run_at=next_run_at,
        summary={
            "total_new": 0,
            "total_refreshed": 0,
            "analysis_count": 0,
            "status": "running",
        },
    )

    total_new = 0
    total_refreshed = 0
    results = {}
    error_messages = []
    successful_banks = 0
    failed_banks = 0
    collection_started_at = utc_now_iso()

    for bank_code in target_banks:
        if bank_code not in ALL_SCRAPERS:
            logger.warning("Unknown bank code: %s", bank_code)
            continue

        logger.info("%s", "=" * 50)
        logger.info("Processing: %s", bank_code)
        logger.info("%s", "=" * 50)

        bank_started_at = utc_now_iso()
        bank_started_perf = time.perf_counter()
        bank_status = "success"
        bank_error = None
        bank_new_count = 0
        bank_refreshed_count = 0
        scraper = None

        log_pipeline_job(
            pipeline_logger,
            f"collect_{bank_code.lower()}",
            "running",
            bank_code=bank_code,
            mode=mode,
        )

        try:
            scraper_cls = ALL_SCRAPERS[bank_code]
            scraper = scraper_cls(db=db)

            bank_refreshed_count = scraper.refresh_incomplete_speeches()
            total_refreshed += bank_refreshed_count
            if bank_refreshed_count:
                logger.info(
                    "[%s] Refreshed %s incomplete speeches",
                    bank_code,
                    bank_refreshed_count,
                )

            if mode == "full":
                bank_new_count = scraper.collect_new_speeches(
                    start_year=start_year,
                    fetch_text=True,
                )
            else:
                bank_new_count = scraper.collect_recent(fetch_text=True)

            results[bank_code] = bank_new_count
            total_new += bank_new_count
            successful_banks += 1
            logger.info(
                "[%s] %s new speeches added",
                bank_code,
                bank_new_count,
            )
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": f"bank:{bank_code}",
                    "status": "success",
                    "run_id": run_id,
                    "message": f"{bank_code} collection finished",
                    "new_items": bank_new_count,
                    "refreshed_items": bank_refreshed_count,
                }
            )
        except Exception as exc:
            bank_status = "failed"
            bank_error = str(exc)
            results[bank_code] = -1
            failed_banks += 1
            error_messages.append(f"{bank_code}: {exc}")
            logger.error("[%s] Pipeline failed: %s", bank_code, exc)
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": f"bank:{bank_code}",
                    "status": "failed",
                    "run_id": run_id,
                    "message": str(exc),
                }
            )
        finally:
            if scraper is not None:
                scraper.close()

            bank_finished_at = utc_now_iso()
            log_pipeline_job(
                pipeline_logger,
                f"collect_{bank_code.lower()}",
                bank_status,
                bank_started_perf,
                bank_code=bank_code,
                new_items=bank_new_count,
                refreshed_items=bank_refreshed_count,
                error_message=bank_error,
                mode=mode,
            )
            try:
                db.log_pipeline_step(
                    run_id=run_id,
                    stage_name=f"bank:{bank_code}",
                    started_at=bank_started_at,
                    finished_at=bank_finished_at,
                    status=bank_status,
                    item_count=bank_new_count,
                    error_msg=bank_error,
                    details={
                        "refreshed_count": bank_refreshed_count,
                        "mode": mode,
                        "new_count": bank_new_count,
                    },
                )
            except Exception as exc:
                logger.error(
                    "Failed to save bank pipeline log for %s: %s",
                    bank_code,
                    exc,
                )

            update_bank(
                bank_code,
                state=bank_status,
                last_run_at=started_at,
                last_success_at=(
                    bank_finished_at if bank_status == "success" else None
                ),
                last_failure_at=(
                    bank_finished_at if bank_status == "failed" else None
                ),
                next_run_at=next_run_at,
                collection={
                    "started_at": bank_started_at,
                    "finished_at": bank_finished_at,
                    "status": bank_status,
                    "new_items": bank_new_count,
                    "updated_items": bank_refreshed_count,
                    "error": bank_error,
                    "mode": mode,
                },
                analysis={
                    "started_at": None,
                    "finished_at": None,
                    "status": "pending" if analyze else "skipped",
                    "analyzed_items": 0,
                    "error": None,
                },
            )

    if failed_banks == 0:
        collection_status = "success"
    elif successful_banks:
        collection_status = "partial"
    else:
        collection_status = "failed"

    collection_finished_at = utc_now_iso()
    update_stage(
        "collection",
        started_at=collection_started_at,
        finished_at=collection_finished_at,
        status=collection_status,
        total_new=total_new,
        total_refreshed=total_refreshed,
        details=results,
    )

    analysis_started_at = utc_now_iso()
    analysis_perf = time.perf_counter()
    analysis_count = 0
    analysis_status = "skipped"
    analysis_error = None
    should_analyze = analyze and (total_new > 0 or total_refreshed > 0)

    log_pipeline_job(
        pipeline_logger,
        "initial_analysis",
        "running" if should_analyze else "skipped",
        analyze_requested=analyze,
        new_or_refreshed_available=(
            total_new > 0 or total_refreshed > 0
        ),
    )

    if should_analyze:
        try:
            from .analyzer import HawkDoveAnalyzer

            analyzer = HawkDoveAnalyzer(db=db)
            analysis_count = analyzer.analyze_pending()
            analysis_status = "success"
            logger.info("Analyzed %s speeches", analysis_count)
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": "analysis",
                    "status": "success",
                    "run_id": run_id,
                    "message": "analysis finished",
                    "analyzed_items": analysis_count,
                }
            )
        except ImportError:
            analysis_status = "skipped"
            logger.warning("Analyzer not available, skipping analysis")
        except Exception as exc:
            analysis_status = "failed"
            analysis_error = str(exc)
            logger.error("Analysis failed: %s", exc)
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": "analysis",
                    "status": "failed",
                    "run_id": run_id,
                    "message": str(exc),
                }
            )

    analysis_finished_at = utc_now_iso()
    log_pipeline_job(
        pipeline_logger,
        "initial_analysis",
        analysis_status,
        analysis_perf,
        analyzed_items=analysis_count,
        error_message=analysis_error,
    )
    db.log_pipeline_step(
        run_id=run_id,
        stage_name="analysis_initial",
        started_at=analysis_started_at,
        finished_at=analysis_finished_at,
        status=analysis_status,
        item_count=analysis_count,
        error_msg=analysis_error,
        details={
            "analyze_requested": analyze,
            "new_or_refreshed_available": (
                total_new > 0 or total_refreshed > 0
            ),
        },
    )
    update_stage(
        "analysis",
        started_at=analysis_started_at,
        finished_at=analysis_finished_at,
        status=analysis_status,
        analyzed_items=analysis_count,
        error=analysis_error,
    )

    for bank_code in target_banks:
        if bank_code in ALL_SCRAPERS:
            update_bank(
                bank_code,
                analysis={
                    "started_at": analysis_started_at,
                    "finished_at": analysis_finished_at,
                    "status": analysis_status,
                    "analyzed_items": analysis_count,
                    "error": analysis_error,
                },
            )

    overall_status = collection_status
    if analysis_status == "failed" and overall_status == "success":
        overall_status = "partial"

    db.log_pipeline_step(
        run_id=run_id,
        stage_name="collection",
        started_at=collection_started_at,
        finished_at=collection_finished_at,
        status=overall_status,
        item_count=total_new,
        error_msg="; ".join(error_messages) or None,
        details={
            "bank_stats": results,
            "total_refreshed": total_refreshed,
            "mode": mode,
            "target_banks": target_banks,
        },
    )
    update_status(
        run_id=run_id,
        state=overall_status,
        next_run_at=next_run_at,
        summary={
            "total_new": total_new,
            "total_refreshed": total_refreshed,
            "analysis_count": analysis_count,
            "status": overall_status,
        },
        stages={
            "collection": {
                "started_at": collection_started_at,
                "finished_at": collection_finished_at,
                "status": collection_status,
                "total_new": total_new,
                "total_refreshed": total_refreshed,
            },
            "analysis": {
                "started_at": analysis_started_at,
                "finished_at": analysis_finished_at,
                "status": analysis_status,
                "analyzed_items": analysis_count,
                "error": analysis_error,
            },
        },
    )

    logger.info("%s", "=" * 50)
    logger.info(
        "COLLECTION SUMMARY - %s",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    logger.info("%s", "=" * 50)
    for bank, count in results.items():
        status_text = f"{count} new" if count >= 0 else "FAILED"
        logger.info("  %s: %s", bank, status_text)
    logger.info("  Total new: %s", total_new)
    logger.info("  Total refreshed: %s", total_refreshed)

    return {
        "run_id": run_id,
        "bank_results": results,
        "total_new": total_new,
        "total_refreshed": total_refreshed,
        "analysis_count": analysis_count,
        "status": overall_status,
        "analysis_status": analysis_status,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Central Bank Speech Collector"
    )
    parser.add_argument(
        "--banks",
        nargs="+",
        choices=list(ALL_SCRAPERS.keys()),
        help="Specific banks to collect (default: all)",
    )
    parser.add_argument(
        "--mode",
        choices=["recent", "full"],
        default="recent",
        help="recent=current year only, full=all available years",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Start year for full mode (default: earliest available)",
    )
    parser.add_argument(
        "--no-analyze",
        action="store_true",
        help="Skip NLP analysis",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database stats and exit",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: fetch speech lists without storing data",
    )
    args = parser.parse_args()

    if args.stats:
        stats = SpeechDB().get_stats()
        print("\n" + "=" * 40)
        print("Central Bank Watchtower - Database Stats")
        print("=" * 40)
        for bank in sorted(stats.keys()):
            if bank == "total":
                continue
            item = stats[bank]
            print(
                f"  {bank}: {item['total_speeches']} speeches "
                f"({item['analyzed']} analyzed)"
            )
        print(f"  Total: {stats['total']} speeches")
        return 0

    if args.test:
        print("Running test mode...")
        db = SpeechDB()
        for bank_code, scraper_cls in ALL_SCRAPERS.items():
            scraper = None
            try:
                scraper = scraper_cls(db=db)
                speeches = scraper.fetch_speech_list()
                if speeches:
                    speech = speeches[0]
                    print(f"\n[{bank_code}] Found {len(speeches)} speeches")
                    print(f"  Title: {speech['title'][:80]}")
                    print(f"  Date: {speech['date']}")
                    print(f"  URL: {speech['url'][:80]}")
                    print(
                        f"  Speaker: {speech.get('speaker', 'N/A')}"
                    )
                else:
                    print(f"\n[{bank_code}] No speeches found")
            except Exception as exc:
                print(f"\n[{bank_code}] ERROR: {exc}")
            finally:
                if scraper is not None:
                    scraper.close()
        return 0

    result = run_collection(
        banks=args.banks,
        mode=args.mode,
        analyze=not args.no_analyze,
        start_year=args.start_year,
    )
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
