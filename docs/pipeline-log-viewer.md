# Pipeline Log Viewer

`log-viewer/`는 중앙은행 speech 파이프라인의 로컬 실행 로그를 확인하기 위한
정적 웹 페이지다.

신규 pipeline 단계는 다음과 같다.

1. Prepare
2. Collect
3. Analyze
4. Finish

PostgreSQL sync, Tableau mart, inactivity 기반 member cleanup 단계는 더 이상
실행하지 않는다. 기존 log viewer의 Maintenance column은 과거 로그 호환을 위해
남아 있을 수 있지만 신규 실행에는 job이 생성되지 않는다.

## 실행

repository root에서 정적 서버를 실행한다.

```bash
python -m http.server 8000
```

브라우저에서 다음 경로를 연다.

```text
http://localhost:8000/log-viewer/
```

viewer는 `logs/app_YYYY-MM-DD.log`에서 가장 최근 pipeline run을 찾아 표시한다.
과거 로그와의 호환성을 위해 이전의 `Starting sync run` / `Finished sync run`
메시지도 읽을 수 있지만, 신규 실행은 `Starting pipeline run` /
`Finished pipeline run`을 사용한다.

상세 실행 이력의 기준은 SQLite `pipeline_logs`다. 터미널에서는 다음 명령을
사용한다.

```bash
python scripts/speech_tracker/report_pipeline.py --limit 5
```
