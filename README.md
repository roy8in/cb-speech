# Central Bank Speech Tracker

Automated, locally-run data pipeline that collects, analyzes, and synchronizes official speeches from the world's major central banks (FRB, ECB, BOE, BOJ, RBA, BOC).

This project tracks central bank communications, performs local natural language processing (NLP) to gauge hawkish/dovish monetary policy stance, and pushes the consolidated structured data to a PostgreSQL database for use in a Tableau dashboard.

## Key Features

- **Automated Collection:** Built-in web scrapers to gather the latest speech metadata, transcripts (including PDFs), and speaker details from 6 major central banks.
- **Local NLP Pipeline:** Uses **Ollama (Llama 3.1)** locally for offline, hardware-accelerated analysis of central bank rhetoric—completely eliminating dependency on paid APIs like OpenAI and ensuring privacy for sensitive data.
- **Hawkish/Dovish Scoring:** Text sentiment is algorithmically scored on a standardized scale to gauge the monetary policy trajectory.
- **Robust Synchronization:** Smart batched synchronization logic that reliably pushing mass amounts of data (speeches, analysis results, and member metadata) from a local `SQLite` database into a remote `PostgreSQL` production database.
- **Background Automation:** Hands-free execution scheduled every 4 hours utilizing macOS `crontab`.

## Project Architecture

1. **`collector.py` / `/scrapers`**: Checks websites for new speeches, downloads content, and applies duplicate management (UPSERT).
2. **`models.py` (Local DB):** Handles local `SQLite` schema logic. Unsynced changes are carefully tracked via a dedicated `synced_at` column to ensure no dropped data.
3. **`sync_and_analyze.py`**: The main orchestration script. Combines downloading, Ollama analysis of pending entries, and PostgreSQL export into a single cohesive pipeline.
4. **`exporter.py`**: Interacts with the production PostgreSQL API. Handles recursive chunking to ensure that extremely large speech transcripts do not cause API timeout errors during bulk INSERTs.

## Setup & Running

This project has been migrated from a GitHub Actions-based automated system to a highly reliable localized execution environment.

### Prerequisites

- macOS runtime environment
- Python 3+ (managed via `.venv`)
- **Ollama** installed and running locally with the `llama3.1` model. 
- `.env` file configured with database API keys:
  ```env
  POSTGRE_API_URL=https://your-database-api-url
  POSTGRE_API_KEY=your_secure_api_key
  ```

### Manual Execution

Trigger a complete end-to-end run manually:

```bash
source .venv/bin/activate
python scripts/speech_tracker/sync_and_analyze.py
```

### Automation

The system is configured to run entirely in the background via local `cron`. The current crontab runs the master script every 4 hours:

```bash
0 */4 * * * cd /Users/kimberlywexler/work/cb-speeches && /Users/kimberlywexler/work/cb-speeches/.venv/bin/python3 /Users/kimberlywexler/work/cb-speeches/scripts/speech_tracker/sync_and_analyze.py >> /Users/kimberlywexler/work/cb-speeches/sync.log 2>&1
```

All standard output and background errors are redirected to `sync.log`.

## Recent Updates
*   **Decoupled Sync Logic**: Added explicit synchronization tracking (`synced_at`) to the analysis results table to guarantee completion regardless of prior status.
*   **Recursive Export Chunking**: Handled PostgreSQL API errors implicitly caused by payload size limits by dynamically halving the batch sizes of failed database chunks until success.
*   **Error Handling**: Upgraded data sanitization logic (correctly handling missing dates, fixing Unicode characters like `£`, resolving path collisions between web texts vs PDF archives).
