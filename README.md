# 중앙은행 연설 수집기

주요 중앙은행의 공식 연설을 수집해 로컬 SQLite에 저장하고, Gemini로
통화정책 성향을 분석하는 파이프라인입니다.

대상 중앙은행은 `FRB`, `ECB`, `BOE`, `BOJ`, `RBA`, `BOC`입니다.

현재 운영 범위는 **수집 → SQLite 저장 → Gemini 분석 → SQLite 저장**까지입니다.
PostgreSQL과 Tableau 연결은 사용하지 않습니다. Snowflake 적재는 별도 단계로
추가할 예정입니다.

## 주요 경로

- `data/speech_tracker/speeches.db`: 로컬 SQLite 데이터베이스
- `data/speech_tracker/backups/`: 일별 SQLite snapshot
- `tools/speech_tracker/scrapers/`: 중앙은행별 스크레이퍼
- `tools/speech_tracker/models.py`: SQLite 스키마와 DB 접근 함수
- `tools/speech_tracker/analyzer.py`: Gemini 기반 통화정책 성향 분석
- `tools/speech_tracker/collector.py`: 수집과 1차 분석 runner
- `tools/speech_tracker/sentiment.py`: sentiment 파생 로직 reference
- `scripts/speech_tracker/sync_and_analyze.py`: 일일 통합 runner
- `scripts/speech_tracker/run_daily_eastern.py`: 미국 동부 20시 cron wrapper
- `scripts/speech_tracker/report_pipeline.py`: 최근 실행 로그 확인
- `log-viewer/`: 로컬 브라우저 pipeline log viewer

## 데이터 흐름

1. 일일 runner가 기존 SQLite DB의 snapshot을 먼저 만듭니다.
2. 중앙은행 홈페이지에서 연설 목록과 본문을 수집합니다.
3. 새 연설은 SQLite `speeches`에 저장합니다.
4. 기존 연설 중 본문이 비어 있거나 불완전한 최근 항목은 다시 확인합니다.
5. Gemini가 분석 가능한 연설을 평가합니다.
6. 결과는 SQLite `analysis_results`에 저장합니다.
7. 실행 상태와 단계별 로그는 SQLite `pipeline_logs`, `state/`, `logs/`에
   기록합니다.

기존 `data/speech_tracker/speeches.db`는 그대로 사용합니다. 이미 수집된
historical speech를 다시 전부 수집할 필요가 없습니다.

### SQLite backup

기존 DB가 있으면 일일 pipeline이 데이터 변경 전에 SQLite backup API로
consistent snapshot을 생성합니다.

```text
data/speech_tracker/backups/speeches_YYYY-MM-DD.db
```

같은 UTC 날짜에 이미 backup이 있으면 다시 만들지 않습니다. backup 파일은
Git에 포함하지 않습니다.

## 분석 결과

기본 모델은 `gemini-2.5-flash`입니다.

- `stance_score`: -1.0 ~ 1.0. 값이 클수록 매파적
- `stance_reason`: 점수 판단 근거
- `keywords`: 주요 경제 개념
- `main_risk`: 가장 중요한 정책 리스크
- `analysis_status`: `scored`, `no_signal`, `skipped`, `pending`, `failed`
- `model_name`: 분석에 사용한 Gemini model
- `analysis_version`: 분석 방법론 버전

현재 분석 방법론 버전은 `hawk_dove_v1`입니다.

`no_signal`은 통화정책 방향 신호가 거의 없는 연설이고, `skipped`는 본문이
없거나 너무 짧아 분석하지 않은 항목입니다. Gemini 분석이 3회 실패하면
`failed`로 종료합니다. `failed`는 더 이상 매일 pending queue를 막지 않습니다.

기존 DB에서 `pending` 상태로 3회 이상 실패한 행도 DB 초기화 시 `failed`로
정리됩니다.

## Member status

`last_speech_date`는 발언 활동도이지 재직 여부가 아닙니다. 따라서 365일 동안
speech가 없다는 이유로 member를 자동 `retired` 처리하지 않습니다.

`active` / `retired` 상태는 공식 roster 또는 실제 임기 정보처럼 재직 상태를
확인할 수 있는 근거가 있을 때만 갱신합니다.

## 데이터 변경 추적

`speeches.updated_at`은 speech row가 마지막으로 변경된 UTC 시각입니다.
기존 DB에는 컬럼을 additive migration으로 추가하고 기존 행은 `fetched_at` 또는
`created_at` 값으로 채웁니다.

향후 Snowflake 증분 적재에서는 다음 변경시각을 사용할 수 있습니다.

- speeches: `updated_at`
- members: `last_updated`
- analysis_results: `analyzed_at`

기존 PostgreSQL 시절의 `synced_at` 컬럼이 로컬 DB에 남아 있을 수 있지만
물리적으로 삭제하지 않습니다. 기존 DB 보호를 위해 그대로 두고 현재 코드에서는
읽거나 쓰지 않습니다. 새 DB schema에는 `synced_at`을 만들지 않습니다.

DB와 `state/`에 새로 기록하는 operational timestamp는 UTC를 사용합니다.
사람이 보는 `logs/app_YYYY-MM-DD.log`는 기존 log viewer와 운영 스케줄을 위해
미국 동부시간을 유지합니다.

## Sentiment 파생 로직

`tools/speech_tracker/sentiment.py`는 향후 Snowflake `RAW → CORE` SQL을 만들 때
비교 기준으로 사용할 Python reference입니다. 외부 시스템으로 업로드하지
않습니다.

### Speech-level events

`SentimentDeriver.get_events_df()`는 speech와 분석 결과를 결합해 speech 1건당
1행을 만듭니다.

주요 컬럼:

- `speech_id`
- `date`
- `bank_code`
- `speaker`
- `title`
- `stance_score`
- `stance_reason`
- `keywords`
- `main_risk`
- `analysis_status`
- `model_name`
- `analysis_version`
- `speech_updated_at`
- `collection_lag_days`
- `analysis_lag_days`

### Daily sentiment

`SentimentDeriver.get_daily_df()`는 `date + bank_code` 기준 일간 상태를
계산합니다.

현재 기준:

- sentiment 귀속일은 `speech_date`
- 하루에 여러 scored speech가 있으면 `stance_score` 단순 평균
- `no_signal`은 일간 score 평균에서 제외
- speech가 없는 날의 `daily_stance_score`는 NULL
- 마지막 유효 일간 score를 `stance_level_locf`로 유지
- 마지막 scored speech 이후 경과일을 계산
- freshness는 반감기 14일
- `freshness_adjusted_stance = stance_level_locf × freshness_weight`
- 초기 `is_score_fresh` 기준은 45일

이 계산은 현재 동작을 보존하기 위한 reference입니다. Snowflake 구축 시 같은
결과를 내는 CORE SQL을 만든 뒤 Python 결과와 비교 검증합니다.

## 향후 Snowflake 구조

권장 구조는 다음과 같습니다.

- `RAW`: speeches, members, analysis_results
- `CORE`: sentiment_events, sentiment_daily
- `VIEW`: 사용 목적별 조회 view

최초 이전 때는 기존 SQLite 데이터를 Snowflake RAW에 full load하고, 이후에는
신규 또는 변경 데이터만 적재하는 방식으로 전환할 수 있습니다. Snowflake
적재 코드는 현재 repository에 아직 추가하지 않습니다.

## 설치

Python 3.10 이상을 권장합니다.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

PDF 본문 추출에는 `pdfplumber`를 사용합니다.

## 환경 변수

`.env` 또는 실행 환경에 Gemini API key를 설정합니다.

```text
GOOGLE_API_KEY_FREE_TIER=...
```

또는:

```text
GOOGLE_API_KEY=...
```

PostgreSQL 또는 Tableau credential은 필요하지 않습니다.

## 실행

일반 운영 runner:

```bash
python scripts/speech_tracker/sync_and_analyze.py
```

파일명은 기존 cron과의 호환성을 위해 유지하고 있지만, 현재 remote sync는
수행하지 않습니다.

수동 수집:

```bash
python tools/speech_tracker/collector.py --mode recent
```

특정 중앙은행:

```bash
python tools/speech_tracker/collector.py --banks RBA
python tools/speech_tracker/collector.py --banks ECB BOE
```

특정 연도부터 backfill:

```bash
python tools/speech_tracker/collector.py --mode full --start-year 2015
```

분석 없이 수집만 실행:

```bash
python tools/speech_tracker/collector.py --mode recent --no-analyze
```

DB 통계:

```bash
python tools/speech_tracker/collector.py --stats
```

최근 pipeline 로그:

```bash
python scripts/speech_tracker/report_pipeline.py --limit 5
```

## 실행 스케줄

운영 cron은 미국 동부시간 20:00에 하루 한 번 실행합니다.

```cron
0 9,10 * * * cd /Users/kimberlywexler/work/cb-speeches && /Users/kimberlywexler/work/cb-speeches/.venv/bin/python3 /Users/kimberlywexler/work/cb-speeches/scripts/speech_tracker/run_daily_eastern.py >> /Users/kimberlywexler/work/cb-speeches/logs/cron.daily-eastern.log 2>&1
```

wrapper가 실제 미국 동부시간이 20시인지 확인하고, 해당 날짜에 정상 완료된
경우에만 `state/daily_runs/YYYY-MM-DD.done` marker를 만듭니다. 수집 또는 분석
실패로 pipeline이 partial이면 non-zero 종료코드를 반환하므로 완료 marker를
만들지 않습니다.

## 운영 상태 확인

브라우저 log viewer:

```bash
python -m http.server 8000
```

그 다음 `http://localhost:8000/log-viewer/`를 엽니다.

터미널에서는 다음을 우선 확인합니다.

```bash
tail -n 100 logs/cron.daily-eastern.log
python scripts/speech_tracker/report_pipeline.py --limit 5
python tools/speech_tracker/collector.py --stats
```

Gemini free-tier quota에 걸리면 `429 RESOURCE_EXHAUSTED`가 발생할 수 있습니다.
일시 실패는 최대 3회까지 재시도되고, 이후 `failed` 상태로 종결됩니다.
