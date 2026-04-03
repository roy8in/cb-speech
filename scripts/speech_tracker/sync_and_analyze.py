#!/usr/bin/env python3
"""
Speech Tracker — Unified Sync and Analyze Script
Collects new speeches from all central banks and runs LLM analysis.
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.collector import run_collection
from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.analyzer import HawkDoveAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("sync_and_analyze")

def main():
    logger.info("Starting Speech Tracker periodic sync...")
    
    # 1. Run collection for all banks (recent only)
    # This will also run a default analysis pass (50 speeches)
    try:
        run_collection(mode='recent', analyze=True)
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        # Continue to analysis anyway in case there are pending items from before

    # 2. Ensure ALL pending speeches are analyzed (in case more than 50 new ones)
    try:
        db = SpeechDB()
        analyzer = HawkDoveAnalyzer(db)
        
        logger.info("Running exhaustive analysis for any remaining pending speeches...")
        total_analyzed = 0
        while True:
            # Run in batches of 100
            count = analyzer.analyze_pending(limit=100)
            total_analyzed += count
            if count == 0:
                break
            logger.info(f"Batch complete. Total analyzed so far: {total_analyzed}")
            
        logger.info(f"Exhaustive analysis complete. Total new speeches analyzed: {total_analyzed}")
        
    except Exception as e:
        logger.error(f"Exhaustive analysis failed: {e}")

    # 3. Update dashboard data
    try:
        logger.info("Updating dashboard data...")
        from scripts.speech_tracker.generate_dashboard_data import update_dashboard
        update_dashboard()
        logger.info("Dashboard data updated successfully.")
    except ImportError:
        logger.warning("generate_dashboard_data.update_dashboard not found or failed to import.")
    except Exception as e:
        logger.error(f"Dashboard update failed: {e}")

    logger.info("Speech Tracker sync and analyze complete.")

if __name__ == "__main__":
    main()
