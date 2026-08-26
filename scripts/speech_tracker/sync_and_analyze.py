#!/usr/bin/env python3
"""Run the local speech collection and analysis pipeline."""

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
    update_stage,
    update_status,
)
from tools.speech_tracker.analyzer import HawkDoveAnalyzer
from tools.speech_tracker.collector import run_collection
from tools.speech_tracker.models import (
    SpeechDB,
    backup_sqlite_database,
    get_db_path,
)
from tools.speech_tracker.pipeline_log import (
    append_summary,
    log_event,
    log_pipeline_job,
    setup_run_logging,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("sync_and_analyze")


def utc_now_iso():
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def count_pending_analysis(db):
    """Count speeches that still require an analysis attempt."""
    conn = db._get_conn()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM speeches s
            LEFT JOIN analysis_results ar ON s.id = ar.speech_id
            WHERE s.full_text IS NOT NULL
              AND length(s.full_text) > 500
              AND (
                  ar.analysis_status IS NULL
                  OR ar.analysis_status = 'pending'
                  OR ar.analysis_status = 'skipped'
              )
            """
        ).fetchone()
        return int(row["count"])
    finally:
        conn.close()


def run_exhaustive_analysis(db):
    """Process pending analysis rows until no processable work remains."""
    if count_pending_analysis(db) == 0:
        return 0

    analyzer = HawkDoveAnalyzer(db)
    if not analyzer.check_api_status():
        raise RuntimeError("Gemini API key is not configured")

    total_analyzed = 0
    while True:
        count = analyzer.analyze_pending(limit=100)
        total_analyzed += count
        if count == 0:
            break
        logger.info(
            "Analysis batch complete. Total analyzed: %s",
            total_analyzed,
        )

    pending_count = count_pending_analysis(db)
    if pending_count:
        raise RuntimeError(
            f"{pending_count} analyzable speech(s) remain pending"
        )

    return total_analyzed


def fail_prepare(pipeline, pipeline_logger, run_id, error):
    """Record a preparation failure and return a non-zero exit code."""
    duration_sec = round(
        time.perf_counter() - pipeline["run_started_perf"],
        3,
    )
    log_pipeline_job(
        pipeline_logger,
        "prepare_environment",
        "failed",
        pipeline["run_started_perf"],
        error_message=str(error),
    )
    log_event(
        pipeline_logger,
        "error",
        "Pipeline preparation failed",
        run_id=run_id,
        error_message=str(error),
    )
    append_summary(
        pipeline["summary_log_path"],
        run_id=run_id,
        started_at=pipeline["run_started_at"],
        status="FAILED",
        duration_sec=duration_sec,
        total_new=0,
        total_refreshed=0,
        analyzed_items=0,
        failed_steps="prepare_environment",
    )
    update_status(
        run_id=run_id,
        state="failed",
        next_run_at=next_daily_eastern_run(),
        summary={
            "status": "failed",
            "failed_steps": "prepare_environment",
        },
    )
    append_event(
        {
            "service": "cb-speeches",
            "stage": "prepare_environment",
            "status": "failed",
            "run_id": run_id,
            "message": str(error),
        }
    )
    return 1


def main():
    logger.info("Starting local speech pipeline...")
    run_id = uuid.uuid4().hex
    pipeline = setup_run_logging(run_id=run_id)
    pipeline_logger = pipeline["logger"]
    failed_steps = []
    total_new = 0
    total_refreshed = 0
    total_analyzed = 0

    log_event(
        pipeline_logger,
        "info",
        "Starting pipeline run",
        run_id=run_id,
        app_log_path=pipeline["app_log_path"],
        summary_log_path=pipeline["summary_log_path"],
        cwd=Path.cwd(),
        script_dir=Path(__file__).resolve().parent,
        python_executable=sys.executable,
        schedule="daily 20:00 America/New_York",
    )
    log_pipeline_job(
        pipeline_logger,
        "prepare_environment",
        "running",
    )
    update_status(
        run_id=run_id,
        state="running",
        next_run_at=next_daily_eastern_run(),
        summary={"status": "running"},
    )
    append_event(
        {
            "service": "cb-speeches",
            "stage": "pipeline",
            "status": "started",
            "run_id": run_id,
            "message": "local pipeline started",
        }
    )

    try:
        backup_path = backup_sqlite_database(get_db_path())
        db = SpeechDB()
    except Exception as exc:
        logger.error("Pipeline preparation failed: %s", exc)
        return fail_prepare(
            pipeline,
            pipeline_logger,
            run_id,
            exc,
        )

    log_pipeline_job(
        pipeline_logger,
        "prepare_environment",
        "success",
        pipeline["run_started_perf"],
        banks="FRB|ECB|BOE|BOJ|RBA|BOC",
        backup_path=backup_path or "new_database",
    )

    collection_perf = time.perf_counter()
    collection_failed = False
    log_pipeline_job(
        pipeline_logger,
        "collection",
        "running",
        mode="recent",
    )

    try:
        collection_result = run_collection(
            mode="recent",
            analyze=True,
            run_id=run_id,
            pipeline_logger=pipeline_logger,
        )
        total_new = collection_result.get("total_new", 0)
        total_refreshed = collection_result.get("total_refreshed", 0)
        collection_failed = collection_result.get("status") != "success"

        log_pipeline_job(
            pipeline_logger,
            "collection",
            "failed" if collection_failed else "success",
            collection_perf,
            total_new=total_new,
            total_refreshed=total_refreshed,
        )
    except Exception as exc:
        collection_failed = True
        logger.error("Collection failed: %s", exc)
        log_pipeline_job(
            pipeline_logger,
            "collection",
            "failed",
            collection_perf,
            error_message=str(exc),
        )
        append_event(
            {
                "service": "cb-speeches",
                "stage": "collection",
                "status": "failed",
                "run_id": run_id,
                "message": str(exc),
            }
        )

    if collection_failed:
        failed_steps.append("collection")

    analysis_started_at = utc_now_iso()
    analysis_perf = time.perf_counter()
    analysis_status = "success"
    analysis_error = None
    log_pipeline_job(
        pipeline_logger,
        "exhaustive_analysis",
        "running",
    )

    try:
        total_analyzed = run_exhaustive_analysis(db)
        logger.info(
            "Exhaustive analysis complete. Analyzed: %s",
            total_analyzed,
        )
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
    except Exception as exc:
        analysis_status = "failed"
        analysis_error = str(exc)
        failed_steps.append("exhaustive_analysis")
        logger.error("Exhaustive analysis failed: %s", exc)
        append_event(
            {
                "service": "cb-speeches",
                "stage": "analysis_exhaustive",
                "status": "failed",
                "run_id": run_id,
                "message": str(exc),
            }
        )

    analysis_finished_at = utc_now_iso()
    log_pipeline_job(
        pipeline_logger,
        "exhaustive_analysis",
        analysis_status,
        analysis_perf,
        analyzed_items=total_analyzed,
        error_message=analysis_error,
    )
    db.log_pipeline_step(
        run_id=run_id,
        stage_name="analysis_exhaustive",
        started_at=analysis_started_at,
        finished_at=analysis_finished_at,
        status=analysis_status,
        item_count=total_analyzed,
        error_msg=analysis_error,
        details={"mode": "exhaustive"},
    )
    update_stage(
        "analysis_exhaustive",
        started_at=analysis_started_at,
        finished_at=analysis_finished_at,
        status=analysis_status,
        analyzed_items=total_analyzed,
        error=analysis_error,
    )

    final_status = "success" if not failed_steps else "partial"
    duration_sec = round(
        time.perf_counter() - pipeline["run_started_perf"],
        3,
    )
    log_pipeline_job(
        pipeline_logger,
        "finish",
        final_status,
        pipeline["run_started_perf"],
        total_new=total_new,
        total_refreshed=total_refreshed,
        analyzed_items=total_analyzed,
        failed_steps="|".join(failed_steps),
    )
    log_event(
        pipeline_logger,
        "info",
        "Finished pipeline run",
        run_id=run_id,
        status=final_status.upper(),
        duration_sec=duration_sec,
        total_new=total_new,
        total_refreshed=total_refreshed,
        analyzed_items=total_analyzed,
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
        failed_steps="|".join(failed_steps),
    )
    update_status(
        run_id=run_id,
        state=final_status,
        next_run_at=next_daily_eastern_run(),
        summary={
            "status": final_status,
            "total_new": total_new,
            "total_refreshed": total_refreshed,
            "analysis_exhaustive": total_analyzed,
            "backup_path": str(backup_path) if backup_path else None,
        },
    )
    append_event(
        {
            "service": "cb-speeches",
            "stage": "pipeline",
            "status": final_status,
            "run_id": run_id,
            "message": "local pipeline finished",
            "analyzed_items": total_analyzed,
        }
    )

    logger.info("Local speech pipeline complete: %s", final_status)
    return 0 if final_status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
