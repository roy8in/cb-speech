#!/usr/bin/env python3
"""
Speech Tracker — Unified Sync and Analyze Script
Collects new speeches from all central banks and runs LLM analysis.
"""

import sys
import logging
import uuid
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.collector import run_collection
from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.analyzer import HawkDoveAnalyzer
from tools.speech_tracker.exporter import PostgreExporter
from tools.speech_tracker.pipeline_log import append_summary, log_event, log_pipeline_job, setup_run_logging
from ops_status import append_event, next_three_hour_run, update_bank, update_stage, update_status

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sync_and_analyze")

def main():
    logger.info("Starting Speech Tracker periodic sync...")
    run_id = uuid.uuid4().hex
    pipeline = setup_run_logging(run_id=run_id)
    pipeline_logger = pipeline["logger"]
    total_new = 0
    total_refreshed = 0
    failed_steps = []
    log_event(
        pipeline_logger,
        "info",
        "Starting sync run",
        run_id=run_id,
        app_log_path=pipeline["app_log_path"],
        summary_log_path=pipeline["summary_log_path"],
        cwd=Path.cwd(),
        script_dir=Path(__file__).resolve().parent,
        python_executable=sys.executable,
        schedule="daily 20:00 America/New_York",
    )
    log_pipeline_job(pipeline_logger, "prepare_environment", "running")
    log_pipeline_job(
        pipeline_logger,
        "prepare_environment",
        "success",
        pipeline["run_started_perf"],
        banks="FRB|ECB|BOE|BOJ|RBA|BOC",
    )
    run_started_at = datetime.now().isoformat()
    update_status(
        run_id=run_id,
        state="running",
        next_run_at=next_three_hour_run(),
        summary={
            "status": "running",
        },
    )
    append_event(
        {
            "service": "cb-speeches",
            "stage": "sync_and_analyze",
            "status": "started",
            "run_id": run_id,
            "message": "periodic sync started",
        }
    )
    
    # 1. Run collection for all banks (recent only)
    # This also runs the initial analysis pass, but skips sync so we only upload once.
    collection_perf = time.perf_counter()
    log_pipeline_job(pipeline_logger, "collection", "running", mode="recent")
    try:
        collection_result = run_collection(
            mode='recent',
            analyze=True,
            sync=False,
            run_id=run_id,
            pipeline_logger=pipeline_logger,
        )
        total_new = collection_result.get("total_new", 0)
        total_refreshed = collection_result.get("total_refreshed", 0)
        collection_status = collection_result.get("status", "success")
        if collection_status not in ("success", "skipped"):
            failed_steps.append("collection")
        log_pipeline_job(
            pipeline_logger,
            "collection",
            collection_status,
            collection_perf,
            total_new=total_new,
            total_refreshed=total_refreshed,
        )
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        failed_steps.append("collection")
        log_pipeline_job(
            pipeline_logger,
            "collection",
            "failed",
            collection_perf,
            error_message=str(e),
        )
        append_event(
            {
                "service": "cb-speeches",
                "stage": "collection",
                "status": "failed",
                "run_id": run_id,
                "message": str(e),
            }
        )
        # Continue to analysis anyway in case there are pending items from before

    # 2. Ensure ALL pending speeches are analyzed (in case more than 50 new ones)
    analysis_started_at = datetime.now().isoformat()
    analysis_started_perf = time.perf_counter()
    total_analyzed = 0
    analysis_status = 'success'
    analysis_error = None
    db = None
    log_pipeline_job(pipeline_logger, "exhaustive_analysis", "running")
    try:
        db = SpeechDB()
        analyzer = HawkDoveAnalyzer(db)
        
        logger.info("Running exhaustive analysis for any remaining pending speeches...")
        while True:
            # Run in batches of 100
            count = analyzer.analyze_pending(limit=100)
            total_analyzed += count
            if count == 0:
                break
            logger.info(f"Batch complete. Total analyzed so far: {total_analyzed}")
            
        logger.info(f"Exhaustive analysis complete. Total new speeches analyzed: {total_analyzed}")
        append_event(
            {
                "service": "cb-speeches",
                "stage": "analysis_exhaustive",
                "status": "success",
                "run_id": run_id,
                "message": "exhaustive analysis finished",
                "analyzed_items": total_analyzed,
            }
        )
    except Exception as e:
        analysis_status = 'failed'
        analysis_error = str(e)
        failed_steps.append("exhaustive_analysis")
        logger.error(f"Exhaustive analysis failed: {e}")
        append_event(
            {
                "service": "cb-speeches",
                "stage": "analysis_exhaustive",
                "status": "failed",
                "run_id": run_id,
                "message": str(e),
            }
        )
    finally:
        analysis_finished_at = datetime.now().isoformat()
        log_pipeline_job(
            pipeline_logger,
            "exhaustive_analysis",
            analysis_status,
            analysis_started_perf,
            analyzed_items=total_analyzed,
            error_message=analysis_error,
        )
        try:
            if db:
                db.log_pipeline_step(
                    run_id=run_id,
                    stage_name='analysis_exhaustive',
                    started_at=analysis_started_at,
                    finished_at=analysis_finished_at,
                    status=analysis_status,
                    item_count=total_analyzed,
                    error_msg=analysis_error,
                    details={
                        'mode': 'exhaustive',
                    }
                )
        except Exception as e:
            logger.error(f"Failed to write exhaustive analysis log: {e}")
        update_stage(
            "analysis_exhaustive",
            started_at=analysis_started_at,
            finished_at=analysis_finished_at,
            status=analysis_status,
            analyzed_items=total_analyzed,
            error=analysis_error,
        )
        for bank_code in ("FRB", "ECB", "BOE", "BOJ", "RBA", "BOC"):
            update_bank(
                bank_code,
                analysis={
                    'started_at': analysis_started_at,
                    'finished_at': analysis_finished_at,
                    'status': analysis_status,
                    'analyzed_items': total_analyzed,
                    'error': analysis_error,
                    'scope': 'exhaustive',
                },
            )

    # 3. Sync with PostgreSQL (once, after all analysis passes are complete)
    sync_started_at = datetime.now().isoformat()
    sync_started_perf = time.perf_counter()
    sync_status = 'skipped'
    sync_error = None
    synced_count = 0
    sync_stats = {}
    log_pipeline_job(pipeline_logger, "postgres_sync", "running", limit=1000)
    try:
        logger.info("Syncing all newly analyzed speeches with PostgreSQL...")
        exporter = PostgreExporter(db=db)
        synced_count = exporter.upload_new_speeches(limit=1000)
        sync_stats = exporter.last_sync_stats
        sync_status = 'success'
        logger.info(f"Synced {synced_count} records to PostgreSQL")
        append_event(
            {
                "service": "cb-speeches",
                "stage": "sync",
                "status": "success",
                "run_id": run_id,
                "message": "postgre sync finished",
                "synced_items": synced_count,
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
        failed_steps.append("postgres_sync")
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
    finally:
        sync_finished_at = datetime.now().isoformat()
        log_pipeline_job(
            pipeline_logger,
            "postgres_sync",
            sync_status,
            sync_started_perf,
            synced_items=synced_count,
            source_synced_items=sync_stats.get("source_total", 0),
            tableau_mart_items=sync_stats.get("tableau_marts", 0),
            mart_events_rows=sync_stats.get("mart_counts", {}).get("events", 0),
            mart_daily_rows=sync_stats.get("mart_counts", {}).get("daily", 0),
            mart_plot_rows=sync_stats.get("mart_counts", {}).get("plot", 0),
            error_message=sync_error,
        )
        try:
            if db:
                db.log_pipeline_step(
                    run_id=run_id,
                    stage_name='sync',
                    started_at=sync_started_at,
                    finished_at=sync_finished_at,
                    status=sync_status,
                    item_count=synced_count,
                    error_msg=sync_error,
                    details={
                        'limit': 1000,
                        'sync_stats': sync_stats,
                    }
                )
        except Exception as e:
            logger.error(f"Failed to write sync log: {e}")
        update_stage(
            "sync",
            started_at=sync_started_at,
            finished_at=sync_finished_at,
            status=sync_status,
            synced_items=synced_count,
            source_synced_items=sync_stats.get("source_total", 0),
            tableau_mart_items=sync_stats.get("tableau_marts", 0),
            mart_counts=sync_stats.get("mart_counts", {}),
            error=sync_error,
        )
        for bank_code in ("FRB", "ECB", "BOE", "BOJ", "RBA", "BOC"):
            update_bank(
                bank_code,
                sync={
                    'started_at': sync_started_at,
                    'finished_at': sync_finished_at,
                    'status': sync_status,
                    'synced_items': synced_count,
                    'source_synced_items': sync_stats.get("source_total", 0),
                    'tableau_mart_items': sync_stats.get("tableau_marts", 0),
                    'error': sync_error,
                    'scope': 'exhaustive',
                },
            )

    logger.info("Speech Tracker sync and analyze complete.")
    final_status = "partial" if analysis_status == "failed" or sync_status == "failed" or failed_steps else "success"
    duration_sec = round(time.perf_counter() - pipeline["run_started_perf"], 3)
    log_pipeline_job(
        pipeline_logger,
        "finish",
        final_status,
        pipeline["run_started_perf"],
        total_new=total_new,
        total_refreshed=total_refreshed,
        analyzed_items=total_analyzed,
        synced_items=synced_count,
        source_synced_items=sync_stats.get("source_total", 0),
        tableau_mart_items=sync_stats.get("tableau_marts", 0),
        mart_events_rows=sync_stats.get("mart_counts", {}).get("events", 0),
        mart_daily_rows=sync_stats.get("mart_counts", {}).get("daily", 0),
        mart_plot_rows=sync_stats.get("mart_counts", {}).get("plot", 0),
        failed_steps="|".join(failed_steps),
    )
    log_event(
        pipeline_logger,
        "info",
        "Finished sync run",
        run_id=run_id,
        status=final_status.upper(),
        duration_sec=duration_sec,
        total_new=total_new,
        total_refreshed=total_refreshed,
        analyzed_items=total_analyzed,
        synced_items=synced_count,
        source_synced_items=sync_stats.get("source_total", 0),
        tableau_mart_items=sync_stats.get("tableau_marts", 0),
        mart_events_rows=sync_stats.get("mart_counts", {}).get("events", 0),
        mart_daily_rows=sync_stats.get("mart_counts", {}).get("daily", 0),
        mart_plot_rows=sync_stats.get("mart_counts", {}).get("plot", 0),
        failed_steps="|".join(failed_steps),
    )
    append_summary(
        pipeline["summary_log_path"],
        run_id=run_id,
        started_at=pipeline["run_started_at"],
        status=final_status.upper(),
        duration_sec=duration_sec,
        total_new=total_new,
        total_refreshed=total_refreshed,
        analyzed_items=total_analyzed,
        synced_items=synced_count,
        failed_steps="|".join(failed_steps),
    )
    update_status(
        run_id=run_id,
        state=final_status,
        next_run_at=next_three_hour_run(),
        summary={
            "status": final_status,
            "analysis_exhaustive": total_analyzed,
            "sync_count": synced_count,
            "source_sync_count": sync_stats.get("source_total", 0),
            "tableau_mart_count": sync_stats.get("tableau_marts", 0),
            "mart_counts": sync_stats.get("mart_counts", {}),
        },
    )
    append_event(
        {
            "service": "cb-speeches",
            "stage": "sync_and_analyze",
            "status": final_status,
            "run_id": run_id,
            "message": "periodic sync finished",
            "analyzed_items": total_analyzed,
            "synced_items": synced_count,
            "source_synced_items": sync_stats.get("source_total", 0),
            "tableau_mart_items": sync_stats.get("tableau_marts", 0),
            "mart_events_rows": sync_stats.get("mart_counts", {}).get("events", 0),
            "mart_daily_rows": sync_stats.get("mart_counts", {}).get("daily", 0),
            "mart_plot_rows": sync_stats.get("mart_counts", {}).get("plot", 0),
        }
    )

if __name__ == "__main__":
    main()
