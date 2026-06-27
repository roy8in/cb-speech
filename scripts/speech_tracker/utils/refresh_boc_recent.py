#!/usr/bin/env python3
"""Refresh recent Bank of Canada speech metadata and cleaned full text."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.speech_tracker.models import SpeechDB
from tools.speech_tracker.scrapers.boc import BOCScraper


def main():
    year = datetime.now().year
    db = SpeechDB()
    scraper = BOCScraper(db=db)
    items = scraper.fetch_speech_list(year=year)

    updated = 0
    skipped_missing_text = 0
    collectable_urls = [item["url"] for item in items]
    for item in items:
        full_text = scraper.fetch_speech_text(item["url"])
        if not full_text:
            skipped_missing_text += 1
            continue

        speaker_id = db.get_or_create_member(scraper.BANK_CODE, item.get("speaker"))
        conn = db._get_conn()
        try:
            cursor = conn.execute(
                """
                UPDATE speeches
                SET title = ?,
                    date = ?,
                    speaker_id = ?,
                    full_text = ?,
                    speech_type = ?,
                    fetched_at = ?,
                    synced_at = NULL
                WHERE url = ?
                """,
                (
                    item["title"],
                    item["date"],
                    speaker_id,
                    full_text,
                    item.get("speech_type", "speech"),
                    datetime.now().isoformat(),
                    item["url"],
                ),
            )
            updated += cursor.rowcount
            conn.commit()
        finally:
            conn.close()

    marked_non_speech = 0
    if collectable_urls:
        conn = db._get_conn()
        try:
            placeholders = ", ".join(["?"] * len(collectable_urls))
            params = [scraper.BANK_CODE, f"{year}-01-01", *collectable_urls]
            cursor = conn.execute(
                f"""
                UPDATE speeches
                SET speech_type = 'non_speech',
                    synced_at = NULL
                WHERE bank_code = ?
                  AND date >= ?
                  AND url NOT IN ({placeholders})
                  AND speech_type = 'speech'
                """,
                params,
            )
            marked_non_speech = cursor.rowcount
            conn.commit()
        finally:
            conn.close()

    print(
        {
            "year": year,
            "candidates": len(items),
            "updated": updated,
            "marked_non_speech": marked_non_speech,
            "skipped_missing_text": skipped_missing_text,
        }
    )


if __name__ == "__main__":
    main()
