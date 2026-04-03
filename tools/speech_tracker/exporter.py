"""
Central Bank Watchtower — Data Exporter

Exports collected speech data to CSV for external use.
Also provides direct access to the SQLite .db file.
"""

import sys
import csv
import shutil
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path(__file__).parent.parent / "data" / "exports"


class DataExporter:
    """Export speech data in multiple formats."""

    def __init__(self, db=None, output_dir=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(self):
        """Export all datasets."""
        files = []
        files.append(self.copy_db())
        logger.info(f"Exported {len(files)} files to {self.output_dir}")
        return files

    def copy_db(self, filename='speeches.db'):
        """
        Copy the SQLite database file to the export directory.
        """
        from .models import get_db_path
        src = get_db_path()
        dst = self.output_dir / filename

        shutil.copy2(src, dst)
        logger.info(f"Copied DB to {dst}")
        return str(dst)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export speech data')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Output directory for exports')
    args = parser.parse_args()

    exporter = DataExporter(output_dir=args.output_dir)

    files = exporter.export_all()

    print(f"\nExported files:")
    for f in files:
        print(f"  {f}")
