# Central Bank Speech Tracker

Python pipeline for collecting official speeches from major central banks, storing them locally in SQLite, analyzing them with Gemini, and syncing the structured data to a PostgreSQL API for downstream reporting.

## What It Does

- Collects speech metadata and full text from 6 central banks: `FRB`, `ECB`, `BOE`, `BOJ`, `RBA`, and `BOC`
- Stores raw and normalized data in a local SQLite database at `data/speech_tracker/speeches.db`
- Uses Google GenAI (`google-genai`) to score speeches on a hawkish/dovish scale and extract key economic themes
- Tracks sync state with `synced_at` fields so only unsynced records are pushed to PostgreSQL
- Handles dynamic pages with Playwright and can extract text from PDFs when `pdfplumber` is available
- Includes a full-text search index on speech titles and transcripts via SQLite FTS5

## Main Components

- `tools/speech_tracker/scrapers/`: Bank-specific scrapers built on a shared base class
- `tools/speech_tracker/models.py`: SQLite schema, migrations, and local data access helpers
- `tools/speech_tracker/analyzer.py`: Gemini-backed stance analysis
- `tools/speech_tracker/exporter.py`: PostgreSQL API sync logic with chunked inserts
- `scripts/speech_tracker/sync_and_analyze.py`: Main end-to-end runner
- `scripts/speech_tracker/reupload_all.py`: Full re-upload workflow for resetting and resyncing data

## Analysis Model

The analyzer currently uses `gemini-2.5-flash` through `google-genai`.

It produces:

- `stance_score`
- `stance_reason`
- `keywords`
- `main_risk`

Speech records are marked as:

- `scored`
- `no_signal`
- `skipped`
- `pending`

## Setup

### Prerequisites

- Python 3.10+
- A Google API key set as either `GOOGLE_API_KEY_FREE_TIER` or `GOOGLE_API_KEY`
- PostgreSQL API credentials set as:
  - `POSTGRE_API_URL`
  - `POSTGRE_API_KEY`

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

If you expect to scrape PDFs, also install:

```bash
pip install pdfplumber
```

## Running

Run the full pipeline:

```bash
source .venv/bin/activate
python scripts/speech_tracker/sync_and_analyze.py
```

Useful collector options:

```bash
python tools/speech_tracker/collector.py --stats
python tools/speech_tracker/collector.py --mode recent
python tools/speech_tracker/collector.py --mode full --start-year 2015
python tools/speech_tracker/collector.py --sync-only
python tools/speech_tracker/collector.py --test
python scripts/speech_tracker/report_pipeline.py --limit 5
```

## Data Flow

1. Scrapers fetch speech lists and, when requested, full speech text.
2. New records are inserted into SQLite.
3. The analyzer processes pending speeches and writes results back locally.
4. The exporter pushes unsynced members, speeches, and analysis results to PostgreSQL.

## Notes

- The repository does not include a committed scheduler configuration. If you want periodic runs, add your own `cron` or Task Scheduler entry.
- The PostgreSQL integration is driven by a custom HTTP API rather than a direct database connection.
- The `POSTGRE_*` environment variable names are intentionally spelled to match the code.
- TLS verification is currently disabled in scraper and exporter requests because the project is used in an environment that may rely on proxy or custom certificate handling. Re-enable it if your deployment has a clean certificate chain.
- Temporary network probes and scrape-debug scripts live under `scripts/speech_tracker/debug/`.
- Stage-level pipeline timings are stored in `pipeline_logs`, which is the single run-log source used by `scripts/speech_tracker/report_pipeline.py`.
