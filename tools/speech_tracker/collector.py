"""
Central Bank Watchtower — Unified Collector

Orchestrates all 6 scrapers, runs analysis, and sends alerts.
Designed for scheduled execution via cron.
"""

import sys
import argparse
import logging
import uuid
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.scrapers import ALL_SCRAPERS
from tools.speech_tracker.exporter import PostgreExporter
from ops_status import (
    append_event,
    iso,
    next_three_hour_run,
    update_bank,
    update_stage,
    update_status,
)
from tools.speech_tracker.pipeline_log import log_pipeline_job

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def run_collection(banks=None, mode='recent', analyze=True, sync=True, start_year=None, run_id=None, pipeline_logger=None):
    """
    Main collection pipeline.

    Args:
        banks: list of bank codes to collect, or None for all
        mode: 'recent' (current year only) or 'full' (all available years)
        analyze: whether to run NLP analysis on new speeches
        sync: whether to sync to PostgreSQL in this run
        start_year: start year for full mode
    """
    db = SpeechDB()
    run_id = run_id or uuid.uuid4().hex
    target_banks = banks or list(ALL_SCRAPERS.keys())
    started_at = datetime.now().isoformat()
    next_run_at = next_three_hour_run()
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
            "sync_count": 0,
            "status": "running",
        },
    )
    
    started_at = datetime.now().isoformat()
    total_new = 0
    total_refreshed = 0
    results = {}
    error_msg = None
    status = 'success'
    collection_started_at = datetime.now().isoformat()

    for bank_code in target_banks:
        if bank_code not in ALL_SCRAPERS:
            logger.warning(f"Unknown bank code: {bank_code}")
            continue

        logger.info(f"{'='*50}")
        logger.info(f"Processing: {bank_code}")
        logger.info(f"{'='*50}")

        bank_started_at = datetime.now().isoformat()
        bank_started_perf = time.perf_counter()
        bank_status = 'success'
        bank_error = None
        bank_new_count = 0
        bank_refreshed_count = 0
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

            # 1. Refresh incomplete speeches (like placeholders)
            refreshed = scraper.refresh_incomplete_speeches()
            total_refreshed += refreshed
            bank_refreshed_count = refreshed
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
            bank_new_count = new_count
            logger.info(f"[{bank_code}] {new_count} new speeches added")
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

        except Exception as e:
            logger.error(f"[{bank_code}] Pipeline failed: {e}")
            results[bank_code] = -1
            bank_status = 'failed'
            bank_error = str(e)
            error_msg = str(e)
            status = 'partial' if total_new > 0 else 'failed'
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": f"bank:{bank_code}",
                    "status": "failed",
                    "run_id": run_id,
                    "message": str(e),
                }
            )
        finally:
            bank_finished_at = datetime.now().isoformat()
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
                    stage_name=f'bank:{bank_code}',
                    started_at=bank_started_at,
                    finished_at=bank_finished_at,
                    status=bank_status,
                    item_count=bank_new_count,
                    error_msg=bank_error,
                    details={
                        'refreshed_count': bank_refreshed_count,
                        'mode': mode,
                        'new_count': bank_new_count,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to save bank pipeline log for {bank_code}: {e}")

            update_bank(
                bank_code,
                state=bank_status,
                last_run_at=started_at,
                last_success_at=bank_finished_at if bank_status == 'success' else None,
                last_failure_at=bank_finished_at if bank_status == 'failed' else None,
                next_run_at=next_run_at,
                collection={
                    'started_at': bank_started_at,
                    'finished_at': bank_finished_at,
                    'status': bank_status,
                    'new_items': bank_new_count,
                    'updated_items': bank_refreshed_count,
                    'error': bank_error,
                    'mode': mode,
                },
                analysis={
                    'started_at': None,
                    'finished_at': None,
                    'status': 'pending' if analyze else 'skipped',
                    'analyzed_items': 0,
                    'error': None,
                },
                sync={
                    'started_at': None,
                    'finished_at': None,
                    'status': 'pending' if sync else 'skipped',
                    'synced_items': 0,
                    'error': None,
                },
            )
            
    # 3. Apply activity-based member status cleanup globally after collection
    member_cleanup_perf = time.perf_counter()
    log_pipeline_job(pipeline_logger, "member_cleanup", "running")
    try:
        from scripts.speech_tracker.migrations.apply_activity_status import apply_activity_based_status
        logger.info("Running activity-based member cleanup...")
        apply_activity_based_status(days_threshold=365)
        log_pipeline_job(
            pipeline_logger,
            "member_cleanup",
            "success",
            member_cleanup_perf,
            days_threshold=365,
        )
    except Exception as e:
        logger.error(f"Failed to run activity status update: {e}")
        log_pipeline_job(
            pipeline_logger,
            "member_cleanup",
            "failed",
            member_cleanup_perf,
            error_message=str(e),
        )
        append_event(
            {
                "service": "cb-speeches",
                "stage": "member_cleanup",
                "status": "failed",
                "run_id": run_id,
                "message": str(e),
            }
        )

    collection_finished_at = datetime.now().isoformat()
    update_stage(
        "collection",
        started_at=collection_started_at,
        finished_at=collection_finished_at,
        status=status,
        total_new=total_new,
        total_refreshed=total_refreshed,
        details=results,
    )

    # Run analysis on new or refreshed speeches
    analysis_started_at = datetime.now().isoformat()
    analysis_started_perf = time.perf_counter()
    analysis_count = 0
    analysis_status = 'skipped'
    analysis_error = None
    log_pipeline_job(
        pipeline_logger,
        "initial_analysis",
        "running" if analyze and (total_new > 0 or total_refreshed > 0) else "skipped",
        analyze_requested=analyze,
        new_or_refreshed_available=total_new > 0 or total_refreshed > 0,
    )
    if analyze and (total_new > 0 or total_refreshed > 0):
        try:
            from .analyzer import HawkDoveAnalyzer
            analyzer = HawkDoveAnalyzer(db=db)
            analysis_count = analyzer.analyze_pending()
            analysis_status = 'success'
            logger.info(f"Analyzed {analysis_count} speeches")
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
            logger.warning("Analyzer not available, skipping analysis")
            analysis_status = 'skipped'
        except Exception as e:
            analysis_status = 'failed'
            analysis_error = str(e)
            logger.error(f"Analysis failed: {e}")
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": "analysis",
                    "status": "failed",
                    "run_id": run_id,
                    "message": str(e),
                }
            )
    analysis_finished_at = datetime.now().isoformat()
    log_pipeline_job(
        pipeline_logger,
        "initial_analysis",
        analysis_status,
        analysis_started_perf,
        analyzed_items=analysis_count,
        error_message=analysis_error,
    )
    db.log_pipeline_step(
        run_id=run_id,
        stage_name='analysis_initial',
        started_at=analysis_started_at,
        finished_at=analysis_finished_at,
        status=analysis_status,
        item_count=analysis_count,
        error_msg=analysis_error,
        details={
            'analyze_requested': analyze,
            'new_or_refreshed_available': total_new > 0 or total_refreshed > 0,
        }
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
                    'started_at': analysis_started_at,
                    'finished_at': analysis_finished_at,
                    'status': analysis_status,
                    'analyzed_items': analysis_count,
                    'error': analysis_error,
                },
            )

    # 4. Sync with PostgreSQL
    sync_count = 0
    sync_started_at = datetime.now().isoformat()
    sync_started_perf = time.perf_counter()
    sync_finished_at = sync_started_at
    sync_status = 'skipped'
    sync_error = None
    sync_stats = {}
    log_pipeline_job(
        pipeline_logger,
        "collection_sync",
        "running" if sync else "skipped",
        sync_requested=sync,
    )
    if sync:
        try:
            logger.info("Syncing new speeches with PostgreSQL...")
            exporter = PostgreExporter(db=db)
            sync_count = exporter.upload_new_speeches()
            sync_stats = exporter.last_sync_stats
            sync_status = 'success'
            logger.info(f"Synced {sync_count} records to PostgreSQL")
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": "sync",
                    "status": "success",
                    "run_id": run_id,
                    "message": "sync finished",
                    "synced_items": sync_count,
                    "source_synced_items": sync_stats.get("source_total", 0),
                    "tableau_mart_items": sync_stats.get("tableau_marts", 0),
                    "mart_events_rows": sync_stats.get("mart_counts", {}).get("events", 0),
                    "mart_daily_rows": sync_stats.get("mart_counts", {}).get("daily", 0),
                    "mart_plot_rows": sync_stats.get("mart_counts", {}).get("plot", 0),
                }
            )
        except Exception as e:
            sync_status = 'failed'
            sync_error = str(e)
            logger.error(f"PostgreSQL sync failed: {e}")
            append_event(
                {
                    "service": "cb-speeches",
                    "stage": "sync",
                    "status": "failed",
                    "run_id": run_id,
                    "message": str(e),
                }
            )
        sync_finished_at = datetime.now().isoformat()
        db.log_pipeline_step(
            run_id=run_id,
            stage_name='sync',
            started_at=sync_started_at,
            finished_at=sync_finished_at,
            status=sync_status,
            item_count=sync_count,
            error_msg=sync_error,
            details={
                'synced_records': sync_count,
                'sync_stats': sync_stats,
            }
        )
    else:
        sync_finished_at = datetime.now().isoformat()
        db.log_pipeline_step(
            run_id=run_id,
            stage_name='sync',
            started_at=sync_started_at,
            finished_at=sync_finished_at,
            status=sync_status,
            item_count=0,
            error_msg=None,
            details={
                'reason': 'sync disabled',
            }
        )
        append_event(
            {
                "service": "cb-speeches",
                "stage": "sync",
                "status": "skipped",
                "run_id": run_id,
                "message": "sync disabled",
            }
        )
    log_pipeline_job(
        pipeline_logger,
        "collection_sync",
        sync_status,
        sync_started_perf,
        synced_items=sync_count,
        source_synced_items=sync_stats.get("source_total", 0),
        tableau_mart_items=sync_stats.get("tableau_marts", 0),
        mart_events_rows=sync_stats.get("mart_counts", {}).get("events", 0),
        mart_daily_rows=sync_stats.get("mart_counts", {}).get("daily", 0),
        mart_plot_rows=sync_stats.get("mart_counts", {}).get("plot", 0),
        error_message=sync_error,
    )
    for bank_code in target_banks:
        if bank_code in ALL_SCRAPERS:
            update_bank(
                bank_code,
                sync={
                    'started_at': sync_started_at,
                    'finished_at': sync_finished_at,
                    'status': sync_status,
                    'synced_items': sync_count,
                    'source_synced_items': sync_stats.get("source_total", 0),
                    'tableau_mart_items': sync_stats.get("tableau_marts", 0),
                    'error': sync_error,
                },
            )

    overall_status = status
    for stage_status in (
        analysis_status if analyze else None,
        sync_status if sync else None,
    ):
        if stage_status == 'failed' and overall_status == 'success':
            overall_status = 'partial'

    finished_at = datetime.now().isoformat()
    
    db.log_pipeline_step(
        run_id=run_id,
        stage_name='collection',
        started_at=collection_started_at,
        finished_at=collection_finished_at,
        status=overall_status,
        item_count=total_new,
        error_msg=error_msg,
        details={
            'bank_stats': results,
            'total_refreshed': total_refreshed,
            'mode': mode,
            'target_banks': target_banks,
        }
    )
    update_status(
        run_id=run_id,
        state=overall_status,
        next_run_at=next_run_at,
        summary={
            'total_new': total_new,
            'total_refreshed': total_refreshed,
            'analysis_count': analysis_count,
            'sync_count': sync_count,
            'status': overall_status,
        },
        stages={
            'collection': {
                'started_at': collection_started_at,
                'finished_at': collection_finished_at,
                'status': status,
                'total_new': total_new,
                'total_refreshed': total_refreshed,
            },
            'analysis': {
                'started_at': analysis_started_at,
                'finished_at': analysis_finished_at,
                'status': analysis_status,
                'analyzed_items': analysis_count,
                'error': analysis_error,
            },
            'sync': {
                'started_at': sync_started_at,
                'finished_at': sync_finished_at,
                'status': sync_status,
                'synced_items': sync_count,
                'error': sync_error,
            },
        }
    )

    # Print summary
    logger.info(f"\n{'='*50}")
    logger.info(f"COLLECTION SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"{'='*50}")
    for bank, count in results.items():
        status_str = f"{count} new" if count >= 0 else "FAILED"
        logger.info(f"  {bank}: {status_str}")
    logger.info(f"  Total new: {total_new}")
    logger.info(f"  Total refreshed: {total_refreshed}")

    return {
        'run_id': run_id,
        'bank_results': results,
        'total_new': total_new,
        'total_refreshed': total_refreshed,
        'analysis_count': analysis_count,
        'sync_count': sync_count,
        'status': overall_status,
        'analysis_status': analysis_status,
        'sync_status': sync_status,
    }


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
    parser.add_argument('--sync-only', action='store_true',
                        help='Only sync unsynced speeches to PostgreSQL and exit')
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

    if args.sync_only:
        db = SpeechDB()
        print("Starting PostgreSQL sync...")
        exporter = PostgreExporter(db=db)
        count = exporter.upload_new_speeches(limit=1000)
        print(f"Successfully synced {count} records to PostgreSQL")
        if exporter.last_sync_stats:
            stats = exporter.last_sync_stats
            mart_counts = stats.get("mart_counts", {})
            print(
                "  Source records: "
                f"{stats.get('source_total', 0)} "
                f"(members={stats.get('members', 0)}, "
                f"speeches={stats.get('speeches', 0)}, "
                f"analysis={stats.get('analysis_results', 0)})"
            )
            print(
                "  Tableau marts: "
                f"{stats.get('tableau_marts', 0)} "
                f"(events={mart_counts.get('events', 0)}, "
                f"daily={mart_counts.get('daily', 0)}, "
                f"plot={mart_counts.get('plot', 0)})"
            )
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
        sync=not args.sync_only,
        start_year=args.start_year,
    )


if __name__ == '__main__':
    main()
