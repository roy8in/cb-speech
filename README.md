# 중앙은행 연설 수집기

주요 중앙은행의 공식 연설 자료를 수집하고, 원문을 저장한 뒤, 통화정책 성향을 분석해 외부 PostgreSQL API로 동기화하는 파이프라인입니다.

대상 중앙은행은 `FRB`, `ECB`, `BOE`, `BOJ`, `RBA`, `BOC`입니다.

## 하는 일

- 각 중앙은행 홈페이지에서 연설 목록, 발표일, 제목, 발표자, 원문 URL을 수집합니다.
- 가능한 경우 연설 본문 전체를 가져와 로컬 SQLite DB에 저장합니다.
- Gemini 모델로 연설의 매파/비둘기파 성향, 판단 근거, 주요 경제 키워드, 핵심 리스크를 분석합니다.
- 로컬 DB의 미동기화 데이터만 PostgreSQL API로 업로드합니다.
- 실행 상태와 이벤트를 `state/` 아래 JSON/JSONL 파일로 남겨 운영 대시보드에서 확인할 수 있게 합니다.

## 주요 경로

- `data/speech_tracker/speeches.db`: 로컬 SQLite 데이터베이스
- `tools/speech_tracker/scrapers/`: 중앙은행별 스크레이퍼
- `tools/speech_tracker/models.py`: SQLite 스키마, 마이그레이션, DB 접근 함수
- `tools/speech_tracker/analyzer.py`: Gemini 기반 통화정책 성향 분석기
- `tools/speech_tracker/exporter.py`: PostgreSQL API 동기화 로직
- `tools/speech_tracker/collector.py`: 수집, 1차 분석, 동기화를 수행하는 공통 runner
- `scripts/speech_tracker/sync_and_analyze.py`: 주기 실행용 통합 runner
- `scripts/speech_tracker/run_daily_eastern.py`: 미국 동부 20시 1일 1회 실행을 보장하는 cron wrapper
- `scripts/speech_tracker/report_pipeline.py`: 최근 파이프라인 실행 로그 확인 도구
- `logs/app_YYYY-MM-DD.log`: log viewer가 읽는 날짜별 structured log
- `logs/summary_YYYY-MM.csv`: 월별 실행 요약 CSV
- `log-viewer/`: 브라우저에서 보는 파이프라인 로그 viewer
- `state/cb_speeches_status.json`: 최신 실행 상태 스냅샷
- `state/cb_speeches_events.jsonl`: 실행 이벤트 스트림

## 데이터 흐름

1. 스크레이퍼가 중앙은행별 목록 페이지에서 최근 또는 전체 연설 목록을 가져옵니다.
2. 새 URL이거나 같은 제목/날짜 조합이 없는 항목만 SQLite에 삽입합니다.
3. 본문이 비어 있거나 너무 짧은 최근 항목은 다음 실행 때 다시 본문 수집을 시도합니다.
4. 분석기는 `pending` 상태의 연설을 Gemini로 분석합니다.
5. 분석 결과는 `analysis_results`에 저장됩니다.
6. exporter가 미동기화된 회원, 연설, 분석 결과를 PostgreSQL API로 업로드합니다.
7. 각 단계의 상태는 SQLite `pipeline_logs`와 `state/` 파일에 기록됩니다.

## 분석 결과

분석 모델은 기본적으로 `gemini-2.5-flash`를 사용합니다.

각 연설에는 다음 분석값이 붙습니다.

- `stance_score`: -1.0에서 1.0 사이의 통화정책 성향 점수. 값이 클수록 매파적입니다.
- `stance_reason`: 점수 판단 근거
- `keywords`: 주요 경제 개념 목록
- `main_risk`: 연설에서 가장 중요하게 언급된 정책 리스크
- `analysis_status`: `scored`, `no_signal`, `skipped`, `pending` 중 하나

`no_signal`은 통화정책 신호가 거의 없는 연설입니다. `skipped`는 본문이 없거나 너무 짧아 분석하지 않은 항목입니다.

## Tableau 및 시장금리 비교 설계 메모

Tableau에서는 매일 존재하는 시장금리 시계열과 간헐적으로 존재하는 연설 sentiment를 같은 날짜 축에서 비교한다. 이때 speech가 없는 날짜에 `stance_score = 0` 같은 값을 저장하거나, Tableau table calculation으로 sparse sentiment의 이동평균을 즉석 계산하면 실제로 존재하지 않는 점수가 만들어진 것처럼 보일 수 있다.

현재 비교 기준은 다음과 같이 둔다.

- sentiment의 경제적 귀속일은 수집일이나 분석일이 아니라 `speech_date`로 둔다. 중앙은행 홈페이지 게시가 늦거나 수집이 늦어도, 연설은 현장, 생중계, 유튜브 등 다른 경로로 이미 시장에 전달됐을 수 있으므로 발표일 기준이 모니터링 목적에 가장 일관된다.
- 시장금리는 speech 발표 당일 종가와 매칭한다.
- 하루에 여러 speech가 있으면 은행별 일간 sentiment는 단순 평균으로 집계한다.
- `analysis_status = 'no_signal'`인 결과는 평균 계산에서 제외한다. 중립값 0으로 넣지 않는다.
- speech가 없는 날짜의 원천 `stance_score`는 NULL로 유지한다.

14일 단순 이동평균 대신 `stance_level_locf`와 `freshness_weight_hl14d`를 분리한다. 최근 14 calendar days 안에 speech가 전혀 없는 경우 단순 이동평균은 끊기거나 표본 수가 급격히 작아지고, carry-forward 값을 다시 계산 입력값으로 넣으면 같은 speech가 반복 반영된다. 따라서 점수 레벨은 마지막 유효 일간 score로 유지하고, 그 점수가 얼마나 오래된 정보인지는 반감기 14일의 신선도 가중치로 따로 표현한다.

이 방식은 "없는 speech를 매일 생성"하지 않는다. speech가 없는 날의 원천 `daily_stance_score`는 NULL이고, Tableau에서 선을 그리기 위한 상태값인 `stance_level_locf`만 유지된다. 오래된 신호를 과대해석하지 않도록 `freshness_weight_hl14d`, `freshness_adjusted_stance`, `is_score_fresh`를 함께 제공한다.

Tableau용 sentiment mart의 권장 파생 컬럼:

- `daily_stance_score`: 해당 날짜에 scored speech가 있을 때만 존재하는 일간 평균
- `daily_scored_speech_count`: 해당 날짜 평균에 포함된 speech 수
- `stance_level_locf`: 최근 유효 일간 score를 다음 scored speech 전까지 유지한 값
- `freshness_weight_hl14d`: 마지막 scored speech 이후 경과일에 반감기 14일을 적용한 신선도 가중치
- `freshness_adjusted_stance`: `stance_level_locf * freshness_weight_hl14d`
- `days_since_last_scored_speech`: 마지막 scored speech 이후 경과일
- `is_score_fresh`: 최근 신호가 너무 오래된 구간을 Tableau에서 숨기거나 흐리게 표시하기 위한 플래그

`is_score_fresh`의 초기 기준은 `days_since_last_scored_speech <= 45` 정도로 두고, 은행별 speech 빈도를 본 뒤 조정한다. 14일 반감기에서는 42일이 지나면 신호 가중치가 약 12.5%까지 낮아지므로, 45일 이후에는 carry-forward된 상태값의 해석력이 크게 약해진다고 볼 수 있다.

며칠 전 speech가 뒤늦게 수집되거나 분석되어 과거 날짜의 sentiment가 새로 생기면, 그 score는 분석일이 아니라 `speech_date`에 반영하고 해당 날짜부터 현재까지 Tableau용 일간 파생값을 다시 계산한다. `collected_at`, `analysis_created_at`, `collection_lag_days`, `analysis_lag_days` 같은 값은 백테스트 기준이 아니라 운영 품질 점검용 메타데이터로 둔다.

PostgreSQL에는 원천 테이블과 별도로 Tableau용 mart 3개를 업로드한다.

`cb_speech_sentiment_events`는 실제 speech 1건당 1행이다. Tableau에서 speech 막대와 hover tooltip의 근거로 사용한다.

- `speech_id`
- `url`
- `date`: `speech_date`
- `bank_code`
- `speaker`
- `title`
- `stance_score`
- `stance_reason`
- `keywords`
- `main_risk`
- `analysis_status`
- `analyzed_at`
- `fetched_at`
- `created_at`
- `collection_lag_days`
- `analysis_lag_days`
- `updated_at`

`cb_speech_sentiment_daily`는 `date + bank_code`당 1행이다. 날짜는 speech가 없는 날도 생성되는 calendar spine에서 나온다. 시장금리, 정책금리 같은 외부 daily table은 이 테이블에 `date + bank_code`로 Tableau relationship 또는 join을 건다.

- `date`
- `bank_code`
- `daily_stance_score`
- `daily_scored_speech_count`
- `daily_total_speech_count`
- `stance_level_locf`
- `freshness_weight_hl14d`
- `freshness_adjusted_stance`
- `last_scored_speech_date`
- `days_since_last_scored_speech`
- `is_score_fresh`
- `has_scored_speech`
- `updated_at`

`cb_speech_sentiment_plot`은 Tableau에서 막대와 선을 같은 날짜축에 안정적으로 올리기 위한 편의 테이블이다. 이 테이블은 공통 `date` 컬럼 하나를 갖고, `mark_type = 'line'` 행은 매일 생성되며 `mark_type = 'speech_bar'` 행은 실제 scored speech가 있는 날에만 생성된다.

- `row_id`
- `date`
- `bank_code`
- `mark_type`: `line` 또는 `speech_bar`
- `speech_id`
- `speaker`
- `title`
- `stance_reason`
- `line_score`: 선으로 그릴 `stance_level_locf`
- `bar_score`: 실제 speech 막대로 그릴 `stance_score`
- `stance_score`
- `freshness_weight_hl14d`
- `freshness_adjusted_stance`
- `days_since_last_scored_speech`
- `is_score_fresh`
- `has_scored_speech`
- `updated_at`

Tableau에서는 `cb_speech_sentiment_plot.date`를 x축으로 사용하고, `mark_type = 'line'`의 `line_score`를 선으로, `mark_type = 'speech_bar'`의 `bar_score`를 막대로 그린다. 막대 hover에는 `speaker`, `title`, `stance_reason`, `stance_score`를 표시한다. 금융시장 데이터는 별도 daily table에서 가져와 `date + bank_code` 기준으로 연결한다.

## 설치

Python 3.10 이상을 권장합니다.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

PDF 본문 추출에는 `pdfplumber`를 사용하며, 기본 의존성에 포함되어 있습니다.

## 환경 변수

`.env` 또는 실행 환경에 다음 값을 설정합니다.

```bash
GOOGLE_API_KEY_FREE_TIER=...
# 또는
GOOGLE_API_KEY=...

POSTGRE_API_URL=...
POSTGRE_API_KEY=...
```

코드에서는 PostgreSQL API 변수명을 `POSTGRE_*`로 사용합니다. 오타처럼 보여도 현재 코드와 맞춘 이름입니다.

## 실행

주기 실행 또는 일반 운영에서는 통합 runner를 사용합니다.

```bash
source .venv/bin/activate
python scripts/speech_tracker/sync_and_analyze.py
```

이 스크립트는 다음 순서로 동작합니다.

1. 전체 중앙은행의 최근 자료를 수집합니다.
2. 수집 단계 내부의 PostgreSQL 동기화는 건너뜁니다.
3. 남아 있는 모든 `pending` 분석 대상을 끝까지 처리합니다.
4. 마지막에 PostgreSQL API로 한 번만 동기화합니다.

## 실행 스케줄

운영 cron은 미국 동부시간 `20:00`에 하루 한 번 수집하도록 설정합니다.

macOS cron은 시스템 로컬 시간 기준으로 동작하므로, 한국 시간 기준으로 동부 20시에 해당할 수 있는 `09:00`, `10:00`에 wrapper를 깨웁니다. wrapper가 현재 미국 동부시간이 20시인지 확인하고, 같은 동부 날짜에 이미 실행된 기록이 있으면 건너뜁니다.

현재 crontab 항목:

```cron
0 9,10 * * * cd /Users/kimberlywexler/work/cb-speeches && /Users/kimberlywexler/work/cb-speeches/.venv/bin/python3 /Users/kimberlywexler/work/cb-speeches/scripts/speech_tracker/run_daily_eastern.py >> /Users/kimberlywexler/work/cb-speeches/logs/cron.daily-eastern.log 2>&1
```

다음 실행 예정 시각은 `state/cb_speeches_status.json`의 `next_run_at`에 미국 동부시간 기준으로 기록됩니다.

## 수동 작업 명령

DB 통계를 확인합니다.

```bash
python tools/speech_tracker/collector.py --stats
```

최근 자료만 수집합니다.

```bash
python tools/speech_tracker/collector.py --mode recent
```

특정 중앙은행만 수집합니다.

```bash
python tools/speech_tracker/collector.py --banks RBA
python tools/speech_tracker/collector.py --banks ECB BOE
```

특정 연도부터 전체 백필을 실행합니다.

```bash
python tools/speech_tracker/collector.py --mode full --start-year 2015
```

분석 없이 수집만 실행합니다.

```bash
python tools/speech_tracker/collector.py --mode recent --no-analyze
```

미동기화 원천 데이터와 Tableau용 sentiment mart를 PostgreSQL API로 업로드합니다.

```bash
python tools/speech_tracker/collector.py --sync-only
```

최근 파이프라인 실행 로그를 확인합니다.

```bash
python scripts/speech_tracker/report_pipeline.py --limit 5
```

## 운영 상태 확인

운영 대시보드용 상태 파일은 `state/`에 기록됩니다.

- `state/cb_speeches_status.json`: 현재 상태, 다음 실행 예정 시간, 은행별 최근 실행 결과
- `state/cb_speeches_events.jsonl`: 실행 시작, 은행별 완료, 분석 완료, 동기화 완료 같은 이벤트

상세 실행 이력과 단계별 소요 시간은 SQLite의 `pipeline_logs` 테이블에 저장됩니다. 사람이 터미널에서 확인할 때는 `scripts/speech_tracker/report_pipeline.py`를 사용합니다.

브라우저에서 파이프라인 로그를 보려면 repo 루트에서 정적 서버를 실행합니다.

```bash
python -m http.server 8000
```

브라우저에서 아래 주소를 엽니다.

```text
http://localhost:8000/log-viewer/
```

log viewer는 `logs/app_YYYY-MM-DD.log`를 읽어 가장 최근 실행 1건을 GitLab pipeline 형태로 표시합니다. 표시되는 주요 job은 준비, 중앙은행별 수집, member cleanup, 분석, PostgreSQL 동기화, 종료 단계입니다.

PostgreSQL 동기화 job에는 원천 증분 업로드 수와 Tableau용 sentiment mart 재생성 행 수가 함께 표시됩니다. 주요 필드는 `synced_items`, `source_synced_items`, `tableau_mart_items`, `mart_events_rows`, `mart_daily_rows`, `mart_plot_rows`입니다. 터미널에서는 `python scripts/speech_tracker/report_pipeline.py --limit 5`로 SQLite `pipeline_logs.details_json`에 저장된 같은 정보를 확인할 수 있습니다.

## 수집 방식 메모

- 대부분의 은행은 `requests`와 `BeautifulSoup`으로 목록과 본문을 파싱합니다.
- 동적 페이지나 차단 우회가 필요한 경우 Playwright를 사용합니다.
- RBA는 Playwright 기반 수집 경로를 사용합니다. 주기 실행 환경에서 asyncio loop가 이미 떠 있어도 별도 스레드에서 Playwright를 실행하도록 처리되어 있습니다.
- TLS 검증은 현재 프록시/인증서 환경을 고려해 일부 요청에서 비활성화되어 있습니다. 배포 환경의 인증서 체인이 안정적이면 재활성화를 검토할 수 있습니다.

## 장애 확인 포인트

수집이 멈춘 것처럼 보이면 먼저 아래를 확인합니다.

```bash
tail -n 100 logs/cron.daily-eastern.log
tail -n 100 logs/app_$(TZ=America/New_York date +%F).log
python scripts/speech_tracker/report_pipeline.py --limit 5
python tools/speech_tracker/collector.py --stats
```

특정 은행의 최신 로컬 저장 상태는 SQLite에서 직접 확인할 수 있습니다.

```bash
.venv/bin/python - <<'PY'
from tools.speech_tracker.models import SpeechDB
conn = SpeechDB()._get_conn()
for row in conn.execute("""
    SELECT date, title, url
    FROM speeches
    WHERE bank_code = 'RBA'
    ORDER BY date DESC
    LIMIT 10
"""):
    print(row["date"], row["title"], row["url"])
conn.close()
PY
```

분석이 오래 걸리거나 일부 실패하는 경우 Google API quota 제한을 확인합니다. free-tier에서는 짧은 시간에 많은 요청이 몰리면 `429 RESOURCE_EXHAUSTED`가 발생할 수 있습니다.

PDF 본문 추출 실패가 보이면 `pdfplumber` 설치 여부를 확인합니다.

```bash
pip install pdfplumber
```

## 개발 및 테스트

테스트는 pytest로 실행합니다.

```bash
pytest
```

가상환경을 사용하는 경우:

```bash
.venv/bin/python -m pytest
```

스크레이퍼 동작만 가볍게 확인하려면:

```bash
python tools/speech_tracker/collector.py --test
```

## 주의사항

- 로컬 SQLite DB와 PostgreSQL API는 `synced_at` 값을 기준으로 증분 동기화합니다.
- 동일 URL은 중복 삽입되지 않습니다.
- URL이 달라도 같은 은행, 제목, 날짜가 같으면 논리적 중복으로 보고 건너뜁니다.
- `state/` 파일은 대시보드용 최신 상태이며, 장기 실행 이력의 기준은 SQLite `pipeline_logs`입니다.
- 디버깅용 HTML과 probe 스크립트는 수집 구조 변화 확인에 사용할 수 있습니다.
