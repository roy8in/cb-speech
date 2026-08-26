import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.speech_tracker.analyzer import (
    ANALYSIS_VERSION,
    MAX_ANALYSIS_ATTEMPTS,
    HawkDoveAnalyzer,
)
from tools.speech_tracker.models import SpeechDB


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)
logger = logging.getLogger(__name__)


def utc_now_iso():
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _next_speech(conn, since_year=None):
    params = []
    date_filter = ""
    if since_year:
        date_filter = " AND s.date >= ?"
        params.append(f"{since_year}-01-01")

    row = conn.execute(
        f"""
        SELECT s.id, s.title, s.date, m.name AS speaker, s.full_text
        FROM speeches s
        LEFT JOIN members m ON s.speaker_id = m.id
        LEFT JOIN analysis_results ar ON s.id = ar.speech_id
        WHERE s.full_text IS NOT NULL
          AND length(s.full_text) > 500
          AND (
              ar.analysis_status IS NULL
              OR ar.analysis_status = 'pending'
          )
          AND (
              ar.analysis_attempts IS NULL
              OR ar.analysis_attempts < ?
          )
          {date_filter}
        ORDER BY
            COALESCE(ar.analysis_attempts, 0) ASC,
            s.date DESC,
            s.id DESC
        LIMIT 1
        """,
        [MAX_ANALYSIS_ATTEMPTS, *params],
    ).fetchone()
    return row


def _save_success(conn, analyzer, speech_id, result):
    status = (
        "scored"
        if result.get("stance_score") is not None
        else "no_signal"
    )
    conn.execute(
        """
        INSERT INTO analysis_results (
            speech_id,
            stance_score,
            stance_reason,
            keywords,
            main_risk,
            analysis_attempts,
            analysis_status,
            analyzed_at,
            model_name,
            analysis_version
        )
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        ON CONFLICT(speech_id) DO UPDATE SET
            stance_score = excluded.stance_score,
            stance_reason = excluded.stance_reason,
            keywords = excluded.keywords,
            main_risk = excluded.main_risk,
            analysis_attempts = COALESCE(
                analysis_results.analysis_attempts,
                0
            ) + 1,
            analysis_status = excluded.analysis_status,
            analyzed_at = excluded.analyzed_at,
            model_name = excluded.model_name,
            analysis_version = excluded.analysis_version
        """,
        (
            speech_id,
            result.get("stance_score"),
            result.get("stance_reason"),
            json.dumps(result.get("keywords")),
            result.get("main_risk"),
            status,
            utc_now_iso(),
            analyzer.model,
            ANALYSIS_VERSION,
        ),
    )
    return status


def _save_failure(conn, analyzer, speech_id):
    conn.execute(
        """
        INSERT INTO analysis_results (
            speech_id,
            analysis_attempts,
            analysis_status,
            analyzed_at,
            model_name,
            analysis_version
        )
        VALUES (?, 1, 'pending', ?, ?, ?)
        ON CONFLICT(speech_id) DO UPDATE SET
            analysis_attempts = COALESCE(
                analysis_results.analysis_attempts,
                0
            ) + 1,
            analysis_status = CASE
                WHEN COALESCE(
                    analysis_results.analysis_attempts,
                    0
                ) + 1 >= ?
                THEN 'failed'
                ELSE 'pending'
            END,
            analyzed_at = excluded.analyzed_at,
            model_name = excluded.model_name,
            analysis_version = excluded.analysis_version
        """,
        (
            speech_id,
            utc_now_iso(),
            analyzer.model,
            ANALYSIS_VERSION,
            MAX_ANALYSIS_ATTEMPTS,
        ),
    )
    return conn.execute(
        """
        SELECT analysis_attempts, analysis_status
        FROM analysis_results
        WHERE speech_id = ?
        """,
        (speech_id,),
    ).fetchone()


def run_analysis(since_year=None, limit=5000, reanalyze=False):
    """Analyze pending speeches until the limit or queue is exhausted."""
    del reanalyze

    db = SpeechDB()
    analyzer = HawkDoveAnalyzer(db)
    analyzer.mark_exhausted_analysis_as_failed()

    if not analyzer.check_api_status():
        print("Error: API key missing.")
        return 1

    conn = db._get_conn()
    success_count = 0
    processed_count = 0
    print("--- Starting Analysis ---")

    try:
        while processed_count < limit:
            row = _next_speech(conn, since_year=since_year)
            if not row:
                print("\n[Done] No more pending speeches.")
                break

            speech_id = row["id"]
            title = row["title"]
            date = row["date"]
            text = row["full_text"]
            speaker = row["speaker"] or "Unknown"
            processed_count += 1

            print(
                f"\n[#{processed_count}] Analyzing ID {speech_id}: "
                f"'{title[:40]}...' ({date})"
            )

            result = analyzer.analyze_text(
                text,
                date=date,
                speaker=speaker,
            )
            if result and isinstance(result, dict):
                status = _save_success(
                    conn,
                    analyzer,
                    speech_id,
                    result,
                )
                conn.commit()
                success_count += 1
                print(
                    f"  -> SUCCESS: status={status}, "
                    f"score={result.get('stance_score')}"
                )
                time.sleep(3)
            else:
                state = _save_failure(conn, analyzer, speech_id)
                conn.commit()
                print(
                    "  -> FAILED ATTEMPT: "
                    f"{state['analysis_attempts']}/"
                    f"{MAX_ANALYSIS_ATTEMPTS}, "
                    f"status={state['analysis_status']}"
                )
                time.sleep(1)

            if success_count > 0 and success_count % 20 == 0:
                db.update_all_member_stances()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=int)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    raise SystemExit(
        run_analysis(
            since_year=args.since,
            limit=args.limit,
        )
    )
