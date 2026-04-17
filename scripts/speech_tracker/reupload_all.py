#!/usr/bin/env python3
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.exporter import PostgreExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("reupload_all")

def main():
    db = SpeechDB()
    exporter = PostgreExporter(db=db)
    prefix = exporter.prefix

    # 1. Drop existing remote tables to ensure clean schema replacement
    logger.info("Dropping existing remote PostgreSQL tables for full replacement...")
    tables_to_drop = [
        f"{prefix}analysis_results",
        f"{prefix}speeches",
        f"{prefix}members"
    ]
    
    for table in tables_to_drop:
        sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
        res = exporter.send_sql(sql)
        if res:
            logger.info(f"Dropped table: {table}")

    # 2. Reset local synced_at flags
    conn = db._get_conn()
    try:
        conn.execute("UPDATE members SET synced_at = NULL")
        conn.execute("UPDATE speeches SET synced_at = NULL")
        conn.execute("UPDATE analysis_results SET synced_at = NULL")
        conn.commit()
        logger.info("Local synced_at flags have been reset.")
    finally:
        conn.close()

    # 3. Perform full sync (this will recreate tables with new schema)
    logger.info("Starting full re-upload to PostgreSQL...")
    total_synced = exporter.sync_all(batch_size=100)
    
    logger.info(f"Re-upload complete. Total records processed: {total_synced}")

if __name__ == "__main__":
    main()
