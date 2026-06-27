# Log-Based Pipeline Viewer Implementation Guide

이 문서는 **로그 파일 하나를 입력으로 받아 GitLab pipelines 형태의 HTML 페이지를 만드는 패턴**을 설명합니다. 특정 프로젝트에 종속되지 않도록 작성했으며, 다른 배치/수집/ETL/리포팅 프로젝트에 그대로 복사해 구현 기준서로 사용할 수 있습니다.

핵심 아이디어는 단순합니다.

1. 파이프라인 실행 중 중요한 작업을 structured log로 남깁니다.
2. 날짜별 log file을 저장합니다.
3. 정적 HTML/JavaScript 페이지가 해당 log file을 읽습니다.
4. 로그 event를 stage/job 구조로 변환합니다.
5. job별 상태, 시작/종료 시각, 소요 시간, 결과, warning/error를 화면에 표시합니다.

이 방식은 별도 DB, backend, log store 없이 시작할 수 있습니다. 처음에는 로컬 정적 페이지로 충분하고, 여러 서버의 로그를 모아야 하는 단계가 오면 같은 event schema를 유지한 채 API나 log store를 붙이면 됩니다.

## 1. 목표

운영자가 날짜 하나를 선택했을 때 아래 질문에 바로 답할 수 있어야 합니다.

- 오늘 파이프라인이 성공했는가?
- 전체 run id는 무엇인가?
- 파이프라인은 몇 시 몇 분 몇 초에 시작했는가?
- 각 작업은 몇 시 몇 분 몇 초에 시작하고 끝났는가?
- 어떤 작업이 성공, 실패, warning, skipped 상태인가?
- 어떤 작업이 오래 걸렸는가?
- 어떤 단계에서 warning/error가 발생했는가?
- 데이터는 몇 row 저장되었는가?
- chunk 처리 결과는 어떤가?
- 산출 파일이나 export 파일은 어디에 저장되었는가?

UI는 GitLab pipelines와 비슷하게 구성합니다.

```text
Prepare        Fetch           Upsert          Export          Derived         Finish
+----------+   +-----------+   +-----------+   +-----------+   +----------+   +---------+
| read cfg |   | fetch src |   | load raw  |   | save file |   | metric A |   | finish  |
| success  |   | success   |   | warning   |   | success   |   | skipped  |   | success |
+----------+   +-----------+   +-----------+   +-----------+   +----------+   +---------+
```

## 2. 권장 파일 구조

프로젝트 루트 기준 권장 구조입니다.

```text
project_root/
  logs/
    app_YYYY-MM-DD.log
    summary_YYYY-MM.csv
  log_viewer/
    index.html
    app.js
    style.css
  docs/
    pipeline-log-viewer.md
```

역할:

- `logs/app_YYYY-MM-DD.log`: 날짜별 상세 event log
- `logs/summary_YYYY-MM.csv`: 월별 run summary
- `log_viewer/index.html`: 화면 구조
- `log_viewer/app.js`: log parsing, job 변환, rendering
- `log_viewer/style.css`: GitLab pipeline 형태의 스타일
- `docs/pipeline-log-viewer.md`: 이 구현 가이드

정적 페이지 실행:

```powershell
python -m http.server 8000
```

브라우저:

```text
http://localhost:8000/log_viewer/
```

페이지는 기본적으로 아래 파일을 읽습니다.

```text
logs/app_YYYY-MM-DD.log
```

## 현재 Stock Consensus viewer 표시 항목

`log_viewer/`는 `stock_consensus/stock_consensus_main.py`가 남기는 날짜별 structured log를 읽어 최신 실행 1건을 단일 pipeline 화면으로 표시합니다. 같은 날짜 로그 파일에 여러 run이 append되어 있어도 마지막 `Starting sync run`부터 해당 `Finished sync run`까지의 구간을 선택합니다.

상단 요약:

- `Status`: 최종 실행 상태입니다. `SUCCESS`, `FAILED`, `WARNING`, `RUNNING` 상태에 따라 색상 배지로 표시합니다.
- `Run ID`: 실행을 구분하는 고유 ID입니다.
- `Started`: 실행 시작 시각입니다.
- `Ended`: 실행 종료 시각입니다.
- `Duration`: 전체 실행 소요 시간입니다.
- `Counts`: 전체 수집 row 수, DB 업로드 row 수, 실패 단계 목록입니다.

`Duration by Job`:

- timed job을 한 줄 누적 막대로 표시합니다.
- 각 조각의 너비는 timed job 전체 소요 시간 중 해당 job이 차지하는 비율입니다.
- skipped job과 0초 job은 timeline에서 제외합니다.
- 각 조각에 마우스를 올리면 작업명, 소요 시간, 상태를 확인할 수 있습니다.
- 상태 의미 색은 유지합니다. `failed`는 빨강, `warning`은 노랑, `skipped`는 회색, `running`은 파랑입니다.
- `success` job은 모두 초록 계열로 표시하되, job끼리 구분되도록 segment마다 다른 초록 tone을 적용합니다.
- segment 사이에는 작은 gap과 흰색 border를 두어 여러 성공 job이 하나의 긴 막대처럼 보이지 않게 합니다.
- legend의 dot도 duration segment와 같은 tone을 사용합니다.

Pipeline columns:

- `Prepare`: 환경 변수 및 설정 로드
- `Session`: Refinitiv session open
- `Fetch`: Refinitiv metadata, consensus, actual, price, event date 수집 및 local CSV cache 갱신
- `Metadata`: `meta_stock_tickers`, `ecs_meta` metadata table 업로드
- `Facts`: `ecs_est`, `ecs_act`, `ecs_price`, `ecs_event_date` fact table 업로드
- `Finish`: 최종 실행 상태

주요 job 카드:

- `load_environment`: `.env`와 DB/Refinitiv 설정 로드
- `open_refinitiv_session`: Refinitiv Workspace session open
- `fetch_refinitiv_data`: Refinitiv data 수집, 가공, `stock_consensus/data_cache/` field-level CSV 저장
- `upsert_metadata_meta_stock_tickers`: `meta_stock_tickers` 업로드
- `upsert_metadata_ecs_meta`: `ecs_meta` 업로드
- `upsert_fact_ecs_est`: estimate fact table 업로드
- `upsert_fact_ecs_act`: actual fact table 업로드
- `upsert_fact_ecs_price`: price fact table 업로드
- `upsert_fact_ecs_event_date`: event date fact table 업로드. source dataframe이 비어 있으면 `skipped`로 표시합니다.
- `close_refinitiv_session`: Refinitiv session close
- `finish`: 전체 run 종료 상태

Warning/Error:

- 상단 `Warnings & Errors` 버튼은 날짜 로그의 warning/error 전체 목록을 보여줍니다.
- job 카드에 issue 버튼이 있으면 해당 job 범위의 warning/error만 확인할 수 있습니다.

Stock Consensus 실행 로그 예:

```text
2026-06-26 11:30:26,852 | INFO | stock_consensus | Starting sync run | run_id=20260626_113026_5749aa64, app_log_path=D:\lseg\logs\app_2026-06-26.log, summary_log_path=D:\lseg\logs\summary_2026-06.csv, cwd=D:\lseg, script_dir=D:\lseg\stock_consensus, python_executable=D:\lseg\.venv\Scripts\python.exe, full_refresh=False
2026-06-26 11:30:26,852 | INFO | stock_consensus | Pipeline job status | job_name=load_environment, status=running
2026-06-26 11:30:26,852 | INFO | stock_consensus | Pipeline job status | job_name=load_environment, status=success, duration_sec=0.0
2026-06-26 11:30:30,577 | INFO | stock_consensus | Pipeline job status | job_name=fetch_refinitiv_data, status=running
2026-06-26 11:33:44,895 | INFO | stock_consensus | Pipeline job status | job_name=fetch_refinitiv_data, status=success, duration_sec=194.318
2026-06-26 11:33:44,895 | INFO | stock_consensus | Collected stock consensus datasets | metadata_rows=189, estimate_rows=433655, actual_rows=1785, price_rows=120640, event_rows=0, records_fetched=556269
2026-06-26 11:38:05,211 | INFO | stock_consensus | Pipeline job status | job_name=upsert_fact_ecs_event_date, status=skipped, table_name=ecs_event_date, reason=dataframe is empty
2026-06-26 11:38:05,215 | INFO | stock_consensus | Finished sync run | run_id=20260626_113026_5749aa64, status=SUCCESS, duration_sec=458.364, records_fetched=556269, rows_uploaded=556458, failed_steps=
```

## 3. 로그에 담아야 할 내용

로그는 크게 5종류의 정보를 담습니다.

### 3.1 Run 정보

파이프라인 전체 실행을 식별하고 요약하기 위한 정보입니다.

권장 필드:

- `run_id`: 실행 고유 ID
- `start_time`: 실행 시작 시각
- `end_time`: 실행 종료 시각
- `duration_sec`: 전체 소요 시간
- `status`: 전체 상태
- `error_stage`: 실패 단계
- `error_message`: 실패 메시지
- `script_dir`: 실행 스크립트 위치
- `cwd`: 실행 working directory
- `python_executable`: Python 실행 파일

예:

```text
2026-06-25 06:00:02,355 | INFO | my_pipeline | Starting sync run | run_id=20260625_060002_a22e895d, cwd=D:\project, python_executable=D:\project\.venv\Scripts\python.exe
```

종료 예:

```text
2026-06-25 06:12:04,642 | INFO | my_pipeline | Finished sync run | run_id=20260625_060002_a22e895d, status=SUCCESS, duration_sec=722.28, rows_uploaded_values=23500, rows_uploaded_derived=144076, chunks_failed=0
```

### 3.2 Job 상태 정보

HTML pipeline 화면의 핵심입니다. 각 job마다 시작과 종료를 남깁니다.

필수 필드:

- `job_name`
- `status`

권장 필드:

- `duration_sec`
- `rows_read`
- `rows_written`
- `rows_uploaded`
- `row_count`
- `chunks_total`
- `chunks_failed`
- `chunks_no_data`
- `output_path`
- `table_name`
- `error_message`

시작:

```text
2026-06-25 06:00:06,188 | INFO | my_pipeline | Pipeline job status | job_name=read_tickers, status=running
```

성공:

```text
2026-06-25 06:00:06,202 | INFO | my_pipeline | Pipeline job status | job_name=read_tickers, status=success, duration_sec=0.014, ticker_total=1961, ticker_valid=1961, ticker_invalid=0
```

실패:

```text
2026-06-25 06:01:24,671 | INFO | my_pipeline | Pipeline job status | job_name=upsert_metadata, status=failed, duration_sec=12.403, table_name=haver_metadata, error_message=upsert returned fewer rows than expected
```

건너뜀:

```text
2026-06-25 06:11:38,424 | INFO | my_pipeline | Pipeline job status | job_name=manufacturing_pmi, status=skipped, reason=no source data updated
```

### 3.3 데이터 처리 결과

작업 결과를 화면 카드와 summary에 표시하기 위한 값입니다.

예:

```text
2026-06-25 06:01:24,671 | INFO | my_pipeline.db | Completed upsert | table_name=haver_metadata, rows_uploaded=224, total_rows=224
2026-06-25 06:05:51,035 | INFO | my_pipeline | Chunk upload complete | frequency=M, chunk_index=2, rows_uploaded=16729
2026-06-25 06:11:38,424 | INFO | my_pipeline.processor | Uploaded CPI YoY Anchor Metrics | row_count=17574
```

권장 필드:

- `table_name`
- `rows_uploaded`
- `total_rows`
- `row_count`
- `frequency`
- `chunk_index`
- `chunk_size`
- `source_count`
- `target_count`

### 3.4 Warning/Error 정보

warning/error는 raw event로 남기되, HTML에서는 기본 화면에 펼치지 않고 drawer나 modal로 drilldown하는 편이 좋습니다.

예:

```text
2026-06-25 06:00:02,367 | WARNING | my_pipeline.db | SSL verification disabled for DB requests
2026-06-25 06:00:06,187 | WARNING | my_pipeline.source | Source path is not configured | source_path_env=False
2026-06-25 06:08:12,100 | ERROR | my_pipeline | Chunk upload failed | frequency=M, chunk_index=4, rows_uploaded=120, expected_rows=500
```

권장 필드:

- `error_message`
- `exception_type`
- `table_name`
- `chunk_index`
- `source_name`
- `retry_count`
- `failed_count`
- `failed_sample`

### 3.5 산출물 정보

파일 export, dashboard publish, materialized view refresh 같은 산출물도 job으로 남깁니다.

예:

```text
2026-06-25 06:09:44,002 | INFO | my_pipeline | Pipeline job status | job_name=export_workbook, status=success, duration_sec=35.8, output_path=state/series_export.xlsx, sheet_count=5
```

권장 필드:

- `output_path`
- `sheet_count`
- `file_size`
- `view_name`
- `refresh_status`
- `publish_url`

## 4. 로그 라인 형식

현재 권장 형식은 사람이 읽기 쉬운 text log + 기계가 읽기 쉬운 key-value suffix입니다.

```text
YYYY-MM-DD HH:MM:SS,mmm | LEVEL | logger_name | message | key=value, key=value
```

예:

```text
2026-06-25 06:00:06,202 | INFO | my_pipeline | Loaded tickers.csv | ticker_total=1961, ticker_valid=1961, ticker_invalid=0
```

구성:

- `timestamp`: Python logging의 `%(asctime)s`
- `level`: `INFO`, `WARNING`, `ERROR`, `EXCEPTION`
- `logger_name`: `my_pipeline`, `my_pipeline.db`, `my_pipeline.processor` 등
- `message`: 사람이 읽는 event 이름
- `extra`: `key=value` 목록

구분자:

- 큰 구분자: ` | `
- extra 내부 구분자: `, `
- key-value 구분자: `=`

주의:

- `message`에는 콤마나 복잡한 값을 넣지 않습니다.
- 기계가 파싱해야 하는 값은 extra에 넣습니다.
- extra value에 `, ` 또는 `=`가 자주 들어가는 값은 짧게 요약하거나 별도 JSON logging을 고려합니다.
- 이 패턴은 간단한 운영용 viewer에 적합합니다. 대규모 로그 검색 시스템에서는 JSON Lines가 더 좋습니다.

## 5. 상태 값 표준

job status는 아래 값을 권장합니다.

| status | 의미 | UI 색상 |
| --- | --- | --- |
| `running` | 시작했지만 종료 event가 아직 없음 | 파란색 |
| `success` | 정상 완료 | 초록색 |
| `warning` | 완료했지만 warning 포함 | 노란색 |
| `failed` | 실패 | 빨간색 |
| `skipped` | 조건상 실행하지 않음 | 회색 |
| `unknown` | 로그 부족으로 상태 추론 불가 | 회색 |

전체 run status는 아래 값을 권장합니다.

| status | 의미 |
| --- | --- |
| `SUCCESS` | 전체 성공 |
| `FAILED` | 전체 실패 |
| `PARTIAL` | 일부 job 실패 또는 누락 |
| `SKIPPED` | 중복 실행 방지 등으로 전체 skip |
| `UNKNOWN` | 종료 로그 없음 |

## 6. Job 이름 규칙

`job_name`은 소문자 snake case로 통일합니다.

좋은 예:

```text
read_config
read_tickers
fetch_metadata
fetch_source_data
upsert_raw_values
calculate_cpi_anchor
export_workbook
refresh_dashboard_view
finish
```

피해야 할 예:

```text
Read Tickers
fetch-metadata
metadata/upsert
JOB1
step2
```

이유:

- snake case는 CSS/JS key로 쓰기 쉽습니다.
- 화면 표시명은 viewer에서 따로 매핑하면 됩니다.
- 이름이 안정적이어야 과거 로그와 새 로그를 함께 읽을 수 있습니다.

## 7. Stage 설계

HTML viewer는 job을 stage column에 배치합니다. stage는 너무 세분화하지 않는 편이 좋습니다.

기본 stage 예:

```javascript
const PIPELINE_COLUMNS = [
  { id: "prepare", label: "Prepare" },
  { id: "fetch", label: "Fetch" },
  { id: "upsert", label: "Upsert" },
  { id: "export", label: "Export" },
  { id: "derived", label: "Derived" },
  { id: "finish", label: "Finish" },
];
```

일반적인 ETL 프로젝트 예:

```javascript
const PIPELINE_COLUMNS = [
  { id: "prepare", label: "Prepare" },
  { id: "extract", label: "Extract" },
  { id: "validate", label: "Validate" },
  { id: "load", label: "Load" },
  { id: "transform", label: "Transform" },
  { id: "publish", label: "Publish" },
  { id: "finish", label: "Finish" },
];
```

권장 stage 수:

- 최소: 4개
- 권장: 5-8개
- 피할 것: 10개 이상

stage가 너무 많으면 한 화면에서 흐름이 보이지 않습니다.

## 8. Python 로깅 구현 예시

아래 코드는 다른 프로젝트에 그대로 옮겨 쓸 수 있는 최소 구현입니다.

```python
import csv
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path


LOG_DIR = Path("logs")
LOGGER_NAME = "my_pipeline"


def _format_extra(extra):
    if not extra:
        return ""
    parts = [f"{key}={value}" for key, value in extra.items()]
    return " | " + ", ".join(parts)


def setup_run_logging():
    LOG_DIR.mkdir(exist_ok=True)

    run_started_at = datetime.now()
    run_id = run_started_at.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    app_log_path = LOG_DIR / f"app_{run_started_at.strftime('%Y-%m-%d')}.log"
    summary_log_path = LOG_DIR / f"summary_{run_started_at.strftime('%Y-%m')}.csv"

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(app_log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return {
        "run_id": run_id,
        "run_started_at": run_started_at,
        "app_log_path": app_log_path,
        "summary_log_path": summary_log_path,
        "logger": logger,
    }


def get_logger(name):
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def log_event(logger, level, message, **extra):
    log_method = getattr(logger, level.lower())
    log_method(f"{message}{_format_extra(extra)}")


def log_pipeline_job(logger, job_name, status, started_at=None, **extra):
    payload = {
        "job_name": job_name,
        "status": status,
    }
    if started_at is not None:
        payload["duration_sec"] = round(time.perf_counter() - started_at, 3)
    payload.update(extra)
    log_event(logger, "info", "Pipeline job status", **payload)
```

### 8.1 Run 시작/종료 로그

```python
run_context = setup_run_logging()
logger = run_context["logger"]
run_started_perf = time.perf_counter()

log_event(
    logger,
    "info",
    "Starting sync run",
    run_id=run_context["run_id"],
    app_log_path=run_context["app_log_path"],
    cwd=Path.cwd(),
)

try:
    run_pipeline(logger)
except Exception as exc:
    log_event(
        logger,
        "exception",
        "Sync run failed with unhandled exception",
        run_id=run_context["run_id"],
        error_message=str(exc),
    )
    raise
finally:
    log_event(
        logger,
        "info",
        "Finished sync run",
        run_id=run_context["run_id"],
        status="SUCCESS",
        duration_sec=round(time.perf_counter() - run_started_perf, 3),
    )
```

### 8.2 Job wrapper 예시

반복되는 job logging은 wrapper로 감싸는 것이 좋습니다.

```python
def run_job(logger, job_name, fn, **context):
    started_at = time.perf_counter()
    log_pipeline_job(logger, job_name, "running", **context)
    try:
        result = fn()
    except Exception as exc:
        log_pipeline_job(
            logger,
            job_name,
            "failed",
            started_at,
            error_message=str(exc),
            **context,
        )
        raise

    extra = result if isinstance(result, dict) else {}
    log_pipeline_job(logger, job_name, "success", started_at, **context, **extra)
    return result
```

사용:

```python
def read_config():
    config = load_config_file()
    return {"config_count": len(config)}


run_job(logger, "read_config", read_config)
```

### 8.3 Skipped job 예시

```python
if not source_updated:
    log_pipeline_job(
        logger,
        "calculate_customer_metrics",
        "skipped",
        reason="no source data updated",
    )
else:
    run_job(logger, "calculate_customer_metrics", calculate_customer_metrics)
```

## 9. Summary CSV

상세 화면은 `app_YYYY-MM-DD.log`를 읽고, 월별 운영 요약은 `summary_YYYY-MM.csv`로 남기는 구성을 추천합니다.

권장 header:

```text
run_id,start_time,end_time,duration_sec,status,rows_read,rows_written,chunks_total,chunks_failed,error_stage,error_message
```

예:

```csv
run_id,start_time,end_time,duration_sec,status,rows_read,rows_written,chunks_total,chunks_failed,error_stage,error_message
20260625_060002_a22e895d,2026-06-25T06:00:02,2026-06-25T06:12:04,722.28,SUCCESS,1961,144076,6,0,,
```

summary CSV는 HTML viewer의 필수 입력은 아니지만, 월간 실행 이력을 빠르게 확인할 때 유용합니다.

## 10. HTML Viewer 구조

정적 viewer는 세 파일로 시작합니다.

```text
log_viewer/
  index.html
  app.js
  style.css
```

### 10.1 index.html 역할

`index.html`은 화면의 고정 구조만 담당합니다.

필수 요소:

- 날짜 input: `#logDate`
- 전체 상태: `#statusText`
- run id: `#runIdText`
- 시작 시각: `#startedText`
- 전체 소요 시간: `#durationText`
- row 요약: `#rowsText`
- pipeline board: `#pipeline`
- warning/error drawer: `#issueDrawer`
- raw event table: `#eventRows`

구조 예:

```html
<input id="logDate" type="date">
<strong id="statusText">-</strong>
<strong id="runIdText">-</strong>
<strong id="startedText">-</strong>
<strong id="durationText">-</strong>
<strong id="rowsText">-</strong>
<div id="pipeline"></div>
<tbody id="eventRows"></tbody>
```

### 10.2 app.js 역할

`app.js`는 아래 순서로 동작합니다.

1. 날짜 input 값을 읽습니다.
2. `../logs/app_YYYY-MM-DD.log`를 fetch합니다.
3. log text를 line 단위로 나눕니다.
4. 각 line을 event object로 파싱합니다.
5. event list를 job list로 변환합니다.
6. job list를 stage column별로 렌더링합니다.
7. warning/error drawer와 raw event table을 렌더링합니다.

핵심 자료구조:

```javascript
{
  timestamp: "2026-06-25 06:00:06,202",
  level: "INFO",
  logger: "my_pipeline",
  message: "Pipeline job status",
  extraText: "job_name=read_tickers, status=success, duration_sec=0.014",
  extra: {
    job_name: "read_tickers",
    status: "success",
    duration_sec: "0.014"
  },
  raw: "..."
}
```

job object:

```javascript
{
  key: "read_tickers",
  column: "prepare",
  label: "Read tickers.csv",
  startTime: "2026-06-25 06:00:06,188",
  endTime: "2026-06-25 06:00:06,202",
  duration: 0.014,
  status: "success",
  detail: "valid 1961 / total 1961",
  events: [...],
  issues: [...]
}
```

### 10.3 style.css 역할

`style.css`는 GitLab pipeline과 유사한 board를 만듭니다.

핵심 스타일:

- 화면 폭은 넓게 사용합니다.
- stage는 가로 column으로 배치합니다.
- job은 card 형태입니다.
- job 왼쪽 border 색으로 상태를 표시합니다.
- warning/error는 drawer로 표시합니다.
- raw event table은 하단에 둡니다.

상태별 색상:

```css
.job.success { border-left-color: #108548; }
.job.warning { border-left-color: #ab6100; }
.job.failed { border-left-color: #dd2b0e; }
.job.running { border-left-color: #1f75cb; }
.job.skipped { border-left-color: #737278; }
```

`Duration by Job`은 상태 의미 색을 유지하되, 성공 job끼리 구분되도록 CSS 변수 `--segment-color`를 사용할 수 있습니다.

```javascript
const SUCCESS_TONES = ["#0b7f44", "#15965a", "#2e7d32", "#23815f", "#4d8f3a", "#007a68"];
```

```css
.duration-timeline {
  gap: 2px;
  padding: 2px;
}

.duration-segment.success {
  background: var(--segment-color, var(--success));
}

.legend-dot.success {
  background: var(--segment-color, var(--success));
}
```

## 11. 로그를 HTML Pipeline으로 변환하는 방법

### 11.1 로그 파싱

라인을 ` | ` 기준으로 나눕니다.

```javascript
function parseLine(line) {
  const parts = line.split(" | ");
  if (parts.length < 4) return null;

  const [timestamp, level, logger, ...messageParts] = parts;
  const rawMessage = messageParts.join(" | ");
  const [message, extraText = ""] = rawMessage.split(" | ", 2);

  return {
    timestamp,
    level: level.trim(),
    logger: logger.trim(),
    message: message.trim(),
    extraText: extraText.trim(),
    raw: line,
    extra: parseExtra(extraText),
  };
}
```

extra는 `, `와 `=`로 파싱합니다.

```javascript
function parseExtra(extraText) {
  const result = {};
  if (!extraText) return result;

  for (const part of extraText.split(", ")) {
    const index = part.indexOf("=");
    if (index <= 0) continue;
    result[part.slice(0, index).trim()] = part.slice(index + 1).trim();
  }
  return result;
}
```

### 11.2 Job 생성

가장 안정적인 방식은 `Pipeline job status` event를 기준으로 job을 만드는 것입니다.

```javascript
const started = events.find(
  event =>
    event.message === "Pipeline job status" &&
    event.extra.job_name === "read_tickers" &&
    event.extra.status === "running"
);

const finished = [...events].reverse().find(
  event =>
    event.message === "Pipeline job status" &&
    event.extra.job_name === "read_tickers" &&
    event.extra.status !== "running"
);
```

이후 job object로 변환합니다.

```javascript
const job = {
  key: "read_tickers",
  column: "prepare",
  label: "Read tickers.csv",
  startTime: started?.timestamp,
  endTime: finished?.timestamp,
  duration: Number(finished?.extra.duration_sec || 0),
  status: finished?.extra.status || "unknown",
  detail: `valid ${finished.extra.ticker_valid} / total ${finished.extra.ticker_total}`,
};
```

### 11.3 과거 로그 fallback

이미 쌓인 로그에 `Pipeline job status`가 없을 수 있습니다. 이 경우 message pattern으로 job을 추론합니다.

예:

```javascript
const start = findFirst(events, ["Fetching metadata"]);
const end = findLast(events, ["Metadata fetch complete"]);
```

이 방식은 기존 로그를 활용할 수 있다는 장점이 있지만, 정확도는 structured job event보다 낮습니다. 새 프로젝트는 처음부터 `Pipeline job status`를 남기는 것을 권장합니다.

### 11.4 Job 상태 추론

명시적 status가 없으면 job 시간 구간의 event level로 추론합니다.

```javascript
function inferJobStatus(jobEvents, hasEnd) {
  if (jobEvents.some(event => ["ERROR", "EXCEPTION"].includes(event.level))) {
    return "failed";
  }
  if (jobEvents.some(event => event.level === "WARNING")) {
    return "warning";
  }
  if (hasEnd) {
    return "success";
  }
  return "running";
}
```

### 11.5 Warning/Error Drilldown

job별 issue는 job 시작~종료 시각 사이의 warning/error event입니다.

```javascript
const issues = jobEvents.filter(
  event => ["WARNING", "ERROR", "EXCEPTION"].includes(event.level)
);
```

UI에서는 기본 화면에 전체 warning/error를 펼치지 않고, 아래 두 방식으로 보여줍니다.

- 상단 `Warnings & Errors` 버튼: 전체 issue
- job card issue 버튼: 해당 job issue

## 12. 다른 프로젝트에 적용하는 순서

1. 이 문서를 프로젝트의 `docs/pipeline-log-viewer.md`로 복사합니다.
2. `logs/` 폴더를 만듭니다.
3. Python logging formatter를 아래 형식으로 맞춥니다.
   - `%(asctime)s | %(levelname)s | %(name)s | %(message)s`
4. `log_event()` helper를 만듭니다.
5. `log_pipeline_job()` helper를 만듭니다.
6. 주요 job마다 `running`, `success`, `failed`, `skipped` event를 남깁니다.
7. `log_viewer/` 폴더를 만듭니다.
8. `index.html`, `app.js`, `style.css`를 추가합니다.
9. `app.js`의 `PIPELINE_COLUMNS`를 프로젝트 stage에 맞춥니다.
10. `app.js`의 job mapping을 프로젝트 job에 맞춥니다.
11. `python -m http.server 8000`으로 viewer를 확인합니다.

## 13. 프로젝트별 커스터마이징 지점

### 13.1 로그 파일 경로

기본:

```javascript
const url = `../logs/app_${dateValue}.log?ts=${Date.now()}`;
```

다른 경로를 쓰면 이 부분만 바꿉니다.

예:

```javascript
const url = `../runtime_logs/pipeline_${dateValue}.log?ts=${Date.now()}`;
```

### 13.2 Stage column

```javascript
const PIPELINE_COLUMNS = [
  { id: "prepare", label: "Prepare" },
  { id: "extract", label: "Extract" },
  { id: "load", label: "Load" },
  { id: "publish", label: "Publish" },
  { id: "finish", label: "Finish" },
];
```

### 13.3 Job mapping

```javascript
const JOBS = [
  {
    key: "read_config",
    column: "prepare",
    label: "Read config",
  },
  {
    key: "extract_orders",
    column: "extract",
    label: "Extract orders",
  },
];
```

### 13.4 Job detail

job detail은 종료 event의 extra에서 만듭니다.

```javascript
detail: end ? `rows ${fmtNumber(end.extra.rows_uploaded)}` : ""
```

프로젝트별로 아래처럼 바꿀 수 있습니다.

```javascript
detail: end ? `files ${fmtNumber(end.extra.file_count)} - rows ${fmtNumber(end.extra.rows_written)}` : ""
```

### 13.5 Duration success tone

성공 job이 여러 개 이어지는 pipeline에서는 전부 같은 초록색이면 timeline segment 구분이 어렵습니다. 이 경우 status 색상 체계는 그대로 두고, `success` segment에만 초록 계열 tone을 순환 적용합니다.

```javascript
const SUCCESS_TONES = ["#0b7f44", "#15965a", "#2e7d32", "#23815f", "#4d8f3a", "#007a68"];

const segmentColor = job.status === "success"
  ? SUCCESS_TONES[index % SUCCESS_TONES.length]
  : "";
const colorStyle = segmentColor ? `; --segment-color: ${segmentColor}` : "";
```

```html
<div
  class="duration-segment success"
  style="flex-basis: 42%; --segment-color: #15965a"
>
  <span>Fetch Refinitiv data</span>
</div>
```

실패/경고/스킵/실행중 job에는 이 tone을 적용하지 않습니다. 운영자가 상태를 색으로 즉시 구분해야 하기 때문입니다.

## 14. 운영 체크리스트

로그 저장:

- 날짜별 app log가 생성된다.
- run 시작 event가 있다.
- run 종료 event가 있다.
- 모든 주요 job에 `Pipeline job status` event가 있다.
- job 종료 event에 `duration_sec`가 있다.
- 저장/업로드 job에 row count가 있다.
- 실패 event에 `error_message`가 있다.

HTML viewer:

- 날짜 선택 시 log file을 읽는다.
- run status가 표시된다.
- run id가 표시된다.
- job card에 `HH:MM:SS -> HH:MM:SS`가 표시된다.
- job card에 소요 시간이 표시된다.
- 성공/실패/warning/skipped가 색으로 구분된다.
- `Duration by Job`의 여러 성공 segment가 초록 계열 tone과 경계선으로 구분된다.
- warning/error drawer가 열린다.
- raw event table 검색이 동작한다.
- level filter가 동작한다.

## 15. 권장 로그 예시 전체

아래는 하나의 간단한 실행 예입니다.

```text
2026-06-25 06:00:02,355 | INFO | my_pipeline | Starting sync run | run_id=20260625_060002_a22e895d, cwd=D:\project
2026-06-25 06:00:02,360 | INFO | my_pipeline | Pipeline job status | job_name=read_config, status=running
2026-06-25 06:00:02,380 | INFO | my_pipeline | Pipeline job status | job_name=read_config, status=success, duration_sec=0.020, config_count=4
2026-06-25 06:00:02,381 | INFO | my_pipeline | Pipeline job status | job_name=extract_source_data, status=running
2026-06-25 06:01:10,120 | INFO | my_pipeline.source | Source fetch complete | row_count=120000
2026-06-25 06:01:10,121 | INFO | my_pipeline | Pipeline job status | job_name=extract_source_data, status=success, duration_sec=67.740, row_count=120000
2026-06-25 06:01:10,122 | INFO | my_pipeline | Pipeline job status | job_name=load_raw_table, status=running, table_name=raw_orders
2026-06-25 06:02:30,511 | WARNING | my_pipeline.db | Some duplicate keys were ignored | table_name=raw_orders, duplicate_count=12
2026-06-25 06:02:45,700 | INFO | my_pipeline | Pipeline job status | job_name=load_raw_table, status=warning, duration_sec=95.578, table_name=raw_orders, rows_uploaded=119988
2026-06-25 06:02:45,701 | INFO | my_pipeline | Pipeline job status | job_name=publish_dashboard_view, status=skipped, reason=no dashboard refresh requested
2026-06-25 06:02:45,702 | INFO | my_pipeline | Finished sync run | run_id=20260625_060002_a22e895d, status=SUCCESS, duration_sec=163.347, rows_uploaded=119988
```

이 로그를 HTML viewer가 읽으면 아래 job들이 생성됩니다.

- `read_config`: success
- `extract_source_data`: success
- `load_raw_table`: warning
- `publish_dashboard_view`: skipped
- `finish`: success

## 16. 확장 방향

처음에는 정적 HTML viewer로 충분합니다. 이후 필요하면 아래 방향으로 확장합니다.

- 여러 서버의 로그를 한 곳에 모으기
- `logs/index.json`을 만들어 날짜 목록 자동 표시
- summary CSV를 읽어 월별 성공률 표시
- failed run만 모아 보기
- materialized view refresh 상태 표시
- Slack/Teams 알림 링크 연결
- JSON Lines 로그 포맷 지원
- backend API로 로그 제공

중요한 것은 UI보다 **event schema를 안정적으로 유지하는 것**입니다. `job_name`, `status`, `duration_sec`, row count, error message만 일관되게 남겨도 대부분의 운영 화면은 쉽게 만들 수 있습니다.
