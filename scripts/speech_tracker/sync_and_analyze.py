#!/usr/bin/env python3
"""
Speech Tracker — Unified Sync and Analyze Script
Collects new speeches from all central banks and runs LLM analysis.
"""

import sys
import logging
import uuid
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.collector import run_collection
from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.analyzer import HawkDoveAnalyzer
from tools.speech_tracker.exporter import PostgreExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sync_and_analyze")

def main():
    logger.info("Starting Speech Tracker periodic sync...")
    run_id = uuid.uuid4().hex
    
    # 1. Run collection for all banks (recent only)
    # This also runs the initial analysis pass, but skips sync so we only upload once.
    try:
        run_collection(mode='recent', analyze=True, sync=False, run_id=run_id)
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        # Continue to analysis anyway in case there are pending items from before

    # 2. Ensure ALL pending speeches are analyzed (in case more than 50 new ones)
    analysis_started_at = datetime.now().isoformat()
    total_analyzed = 0
    analysis_status = 'success'
    analysis_error = None
    db = None
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
    except Exception as e:
        analysis_status = 'failed'
        analysis_error = str(e)
        logger.error(f"Exhaustive analysis failed: {e}")
    finally:
        analysis_finished_at = datetime.now().isoformat()
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

    # 3. Sync with PostgreSQL (once, after all analysis passes are complete)
    sync_started_at = datetime.now().isoformat()
    sync_status = 'skipped'
    sync_error = None
    synced_count = 0
    try:
        logger.info("Syncing all newly analyzed speeches with PostgreSQL...")
        exporter = PostgreExporter(db=db)
        synced_count = exporter.upload_new_speeches(limit=1000)
        sync_status = 'success'
        logger.info(f"Synced {synced_count} speeches to PostgreSQL")
    except Exception as e:
        sync_status = 'failed'
        sync_error = str(e)
        logger.error(f"PostgreSQL sync failed: {e}")
    finally:
        sync_finished_at = datetime.now().isoformat()
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
                    }
                )
        except Exception as e:
            logger.error(f"Failed to write sync log: {e}")

    logger.info("Speech Tracker sync and analyze complete.")

if __name__ == "__main__":
    main()
