"""
Central Bank Watchtower — Data Exporter

Exports collected speech data to PostgreSQL via API and CSV.
"""

import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from core.config import config

logger = logging.getLogger(__name__)

class PostgreExporter:
    """PostgreSQL API를 통한 데이터 동기화 (db_utils.py 스타일)"""

    def __init__(self, db=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.api_url = config.POSTGRE_API_URL
        self.api_key = config.POSTGRE_API_KEY
        self.header = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.prefix = "cb_speech_"
        self.last_sync_stats = {}
        self.last_mart_counts = {}

    def send_sql(self, sql_text):
        """db_utils.py 호환 SQL 전송 함수"""
        if not self.api_url or not self.api_key:
            logger.warning("PostgreSQL API URL or Key missing in .env")
            return None
        
        # API가 'sql' 필드를 기대함 (기존 'query' 아님)
        payload = {"sql": sql_text}
        try:
            # TLS verification is intentionally disabled for the current proxy/certificate environment.
            response = requests.post(self.api_url, json=payload, headers=self.header, verify=False, timeout=60)
            if response.status_code != 200:
                logger.error(f"API Error ({response.status_code}): {response.text[:500]}")
                return None
            res_json = response.json()
            # API returns {'status': 'error', 'message': '...'} on SQL errors
            if res_json.get("status") == "error":
                logger.error(f"SQL Error: {res_json.get('message', 'unknown')[:300]}")
                return None
            return res_json
        except Exception as e:
            logger.error(f"API Request Failed: {e}")
            return None

    def create_table_from_df(self, df, table_name):
        """DataFrame 구조를 기반으로 테이블 생성"""
        columns_sql = []
        for col_name, dtype in df.dtypes.items():
            col_lower = col_name.lower()
            sql_type = 'TEXT'

            if col_lower == 'date':
                sql_type = 'DATE'
            elif 'score' in col_lower:
                sql_type = 'REAL'
            # ID 필드는 항상 BIGINT로 강제 (Tableau 연결 및 타입 일치 보장)
            elif col_lower == 'id' or col_lower.endswith('_id'):
                sql_type = 'BIGINT'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP'
            elif pd.api.types.is_integer_dtype(dtype):
                sql_type = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = 'DOUBLE PRECISION'
            
            # Primary Key 설정
            pk = ""
            if col_lower == 'url' and not table_name.endswith("members"):
                pk = " PRIMARY KEY"
            elif col_lower == 'id' and table_name.endswith("members"):
                pk = " PRIMARY KEY"
            
            columns_sql.append(f'"{col_name}" {sql_type}{pk}')

        # 복합 키 처리 (members 테이블용)
        extra_constraints = ""
        if table_name.endswith("members"):
            if "id" not in [c.lower() for c in df.columns]:
                extra_constraints = f', PRIMARY KEY ("bank_code", "name")'
            else:
                # id가 PK인 경우에도 bank_code + name은 유니크해야 함
                extra_constraints = f', UNIQUE ("bank_code", "name")'

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_sql)}
            {extra_constraints}
        );
        """
        return self.send_sql(create_sql)

    def bulk_insert_df(self, df, table_name):
        """db_utils.py 스타일의 Bulk Insert (UPSERT 지원, 자동 청킹)"""
        if df.empty:
            return 0
        
        # 테이블 생성 보장
        self.create_table_from_df(df, table_name)

        col_names = ', '.join([f'"{c}"' for c in df.columns])
        
        # UPSERT 로직 추가 (ON CONFLICT)
        conflict_target = '"url"'
        if table_name.endswith("members"):
            if "id" in df.columns:
                conflict_target = '"id"'
            else:
                conflict_target = '"bank_code", "name"'
        
        # 업데이트에서 제외할 키 컬럼들
        key_cols = ['url', 'bank_code', 'name', 'id', 'speech_id']
        update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c.lower() not in key_cols]

        return self._insert_chunk(df, table_name, col_names, conflict_target, update_cols)

    def _insert_chunk(self, df, table_name, col_names, conflict_target, update_cols):
        """재귀적 청킹: 실패 시 절반으로 분할하여 재시도"""
        if df.empty:
            return 0

        values_list = []
        for row in df.itertuples(index=False, name=None):
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, (int, float, complex)) and not isinstance(val, bool):
                    # 숫자는 따옴표 없이 (단, Int64 등 pandas 타입 대응을 위해 str 변환 후 검사)
                    row_values.append(f"{val}")
                elif isinstance(val, str):
                    safe_str = val.replace("'", "''")
                    row_values.append(f"'{safe_str}'")
                else: 
                    row_values.append(f"'{val}'")
            values_list.append(f"({', '.join(row_values)})")

        all_values = ', '.join(values_list)
        
        insert_sql = f"""
        INSERT INTO {table_name} ({col_names}) 
        VALUES {all_values}
        ON CONFLICT ({conflict_target}) DO UPDATE SET
            {', '.join(update_cols)};
        """
        res = self.send_sql(insert_sql)
        if res:
            return len(df)
        
        # 실패 시: 1건이면 포기, 그렇지 않으면 절반으로 분할 재시도
        if len(df) <= 1:
            logger.warning(f"Failed to insert 1 row into {table_name}, skipping")
            return 0
        
        mid = len(df) // 2
        logger.info(f"Batch of {len(df)} failed, splitting into {mid} + {len(df) - mid}")
        count_a = self._insert_chunk(df.iloc[:mid], table_name, col_names, conflict_target, update_cols)
        count_b = self._insert_chunk(df.iloc[mid:], table_name, col_names, conflict_target, update_cols)
        return count_a + count_b

    def bulk_upsert_df(self, df, table_name, conflict_target, update_key_cols=None, chunk_size=500):
        """Bulk UPSERT with an explicit conflict target for Tableau mart tables."""
        if df.empty:
            return 0

        col_names = ', '.join([f'"{c}"' for c in df.columns])
        key_cols = {c.lower() for c in (update_key_cols or [])}
        update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c.lower() not in key_cols]
        total = 0
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            total += self._insert_chunk(chunk, table_name, col_names, conflict_target, update_cols)
        return total

    def create_sentiment_mart_tables(self):
        """Create Tableau-facing sentiment mart tables with stable primary keys."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.prefix}sentiment_events (
            "speech_id" BIGINT PRIMARY KEY,
            "url" TEXT,
            "date" DATE,
            "bank_code" TEXT,
            "speaker" TEXT,
            "title" TEXT,
            "stance_score" REAL,
            "stance_reason" TEXT,
            "keywords" TEXT,
            "main_risk" TEXT,
            "analysis_status" TEXT,
            "analyzed_at" TIMESTAMP,
            "fetched_at" TIMESTAMP,
            "created_at" TIMESTAMP,
            "collection_lag_days" BIGINT,
            "analysis_lag_days" BIGINT,
            "updated_at" TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {self.prefix}sentiment_daily (
            "date" DATE NOT NULL,
            "bank_code" TEXT NOT NULL,
            "daily_stance_score" REAL,
            "daily_scored_speech_count" BIGINT,
            "daily_total_speech_count" BIGINT,
            "stance_level_locf" REAL,
            "freshness_weight_hl14d" REAL,
            "freshness_adjusted_stance" REAL,
            "last_scored_speech_date" DATE,
            "days_since_last_scored_speech" BIGINT,
            "is_score_fresh" BIGINT,
            "has_scored_speech" BIGINT,
            "updated_at" TIMESTAMP,
            PRIMARY KEY ("date", "bank_code")
        );

        CREATE TABLE IF NOT EXISTS {self.prefix}sentiment_plot (
            "row_id" TEXT PRIMARY KEY,
            "date" DATE NOT NULL,
            "bank_code" TEXT NOT NULL,
            "mark_type" TEXT NOT NULL,
            "speech_id" BIGINT,
            "speaker" TEXT,
            "title" TEXT,
            "stance_reason" TEXT,
            "line_score" REAL,
            "bar_score" REAL,
            "stance_score" REAL,
            "freshness_weight_hl14d" REAL,
            "freshness_adjusted_stance" REAL,
            "days_since_last_scored_speech" BIGINT,
            "is_score_fresh" BIGINT,
            "has_scored_speech" BIGINT,
            "updated_at" TIMESTAMP
        );
        """
        return self.send_sql(sql)

    def get_sentiment_events_df(self):
        """Speech-level sentiment rows for Tableau bars and tooltips."""
        conn = self.db._get_conn()
        try:
            rows = conn.execute("""
                SELECT
                    s.id AS speech_id,
                    s.url,
                    substr(s.date, 1, 10) AS date,
                    s.bank_code,
                    m.name AS speaker,
                    s.title,
                    ar.stance_score,
                    ar.stance_reason,
                    ar.keywords,
                    ar.main_risk,
                    ar.analysis_status,
                    ar.analyzed_at,
                    s.fetched_at,
                    s.created_at
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE ar.analysis_status IN ('scored', 'no_signal')
                ORDER BY s.bank_code, date, s.id
            """).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
        finally:
            conn.close()

        if df.empty:
            return df

        now = datetime.now().isoformat()
        df['speech_id'] = pd.to_numeric(df['speech_id'], errors='coerce').astype('Int64')
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date.astype('string')
        for col in ['analyzed_at', 'fetched_at', 'created_at']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        speech_date = pd.to_datetime(df['date'], errors='coerce')
        df['collection_lag_days'] = (df['fetched_at'].dt.normalize() - speech_date).dt.days.astype('Int64')
        df['analysis_lag_days'] = (df['analyzed_at'].dt.normalize() - speech_date).dt.days.astype('Int64')
        df['updated_at'] = now

        return df[[
            'speech_id', 'url', 'date', 'bank_code', 'speaker', 'title',
            'stance_score', 'stance_reason', 'keywords', 'main_risk',
            'analysis_status', 'analyzed_at', 'fetched_at', 'created_at',
            'collection_lag_days', 'analysis_lag_days', 'updated_at'
        ]]

    def get_sentiment_daily_df(self, half_life_days=14, fresh_days=45):
        """Bank-date calendar spine with daily sentiment state for Tableau lines."""
        events = self.get_sentiment_events_df()
        if events.empty:
            return events

        scored = events[
            (events['analysis_status'] == 'scored') &
            (events['stance_score'].notna())
        ].copy()
        scored['date'] = pd.to_datetime(scored['date'], errors='coerce')

        speech_counts = (
            events.assign(date=pd.to_datetime(events['date'], errors='coerce'))
            .groupby(['bank_code', 'date'], as_index=False)
            .agg(daily_total_speech_count=('speech_id', 'count'))
        )

        daily_scores = (
            scored.groupby(['bank_code', 'date'], as_index=False)
            .agg(
                daily_stance_score=('stance_score', 'mean'),
                daily_scored_speech_count=('speech_id', 'count'),
            )
        )

        banks = sorted(events['bank_code'].dropna().unique())
        end_date = pd.Timestamp.today().normalize()
        frames = []
        for bank in banks:
            bank_dates = pd.to_datetime(events.loc[events['bank_code'] == bank, 'date'], errors='coerce')
            start_date = bank_dates.min()
            if pd.isna(start_date):
                continue
            frame = pd.DataFrame({
                'date': pd.date_range(start_date.normalize(), end_date, freq='D'),
                'bank_code': bank,
            })
            frames.append(frame)

        if not frames:
            return pd.DataFrame()

        daily = pd.concat(frames, ignore_index=True)
        daily = daily.merge(daily_scores, how='left', on=['bank_code', 'date'])
        daily = daily.merge(speech_counts, how='left', on=['bank_code', 'date'])
        daily['daily_scored_speech_count'] = daily['daily_scored_speech_count'].fillna(0).astype('Int64')
        daily['daily_total_speech_count'] = daily['daily_total_speech_count'].fillna(0).astype('Int64')
        daily['has_scored_speech'] = (daily['daily_scored_speech_count'] > 0).astype('Int64')

        daily = daily.sort_values(['bank_code', 'date'])
        daily['stance_level_locf'] = daily.groupby('bank_code')['daily_stance_score'].ffill()
        daily['last_scored_speech_date'] = daily['date'].where(daily['has_scored_speech'].eq(1))
        daily['last_scored_speech_date'] = daily.groupby('bank_code')['last_scored_speech_date'].ffill()
        daily['days_since_last_scored_speech'] = (
            daily['date'] - daily['last_scored_speech_date']
        ).dt.days.astype('Int64')

        daily['freshness_weight_hl14d'] = 0.5 ** (
            daily['days_since_last_scored_speech'].astype(float) / float(half_life_days)
        )
        daily.loc[daily['days_since_last_scored_speech'].isna(), 'freshness_weight_hl14d'] = pd.NA
        daily['freshness_adjusted_stance'] = daily['stance_level_locf'] * daily['freshness_weight_hl14d']
        daily['is_score_fresh'] = (
            daily['days_since_last_scored_speech'].notna() &
            (daily['days_since_last_scored_speech'] <= fresh_days)
        ).astype('Int64')

        now = datetime.now().isoformat()
        daily['date'] = daily['date'].dt.date.astype('string')
        daily['last_scored_speech_date'] = daily['last_scored_speech_date'].dt.date.astype('string')
        daily['updated_at'] = now

        return daily[[
            'date', 'bank_code', 'daily_stance_score', 'daily_scored_speech_count',
            'daily_total_speech_count', 'stance_level_locf', 'freshness_weight_hl14d',
            'freshness_adjusted_stance', 'last_scored_speech_date',
            'days_since_last_scored_speech', 'is_score_fresh',
            'has_scored_speech', 'updated_at'
        ]]

    def get_sentiment_plot_df(self):
        """Single-date-axis table for Tableau dual mark charts."""
        daily = self.get_sentiment_daily_df()
        events = self.get_sentiment_events_df()
        if daily.empty:
            return pd.DataFrame()

        line = pd.DataFrame({
            'row_id': 'line:' + daily['bank_code'].astype(str) + ':' + daily['date'].astype(str),
            'date': daily['date'],
            'bank_code': daily['bank_code'],
            'mark_type': 'line',
            'speech_id': pd.Series([pd.NA] * len(daily), dtype='Int64'),
            'speaker': pd.NA,
            'title': pd.NA,
            'stance_reason': pd.NA,
            'line_score': daily['stance_level_locf'],
            'bar_score': pd.NA,
            'stance_score': pd.NA,
            'freshness_weight_hl14d': daily['freshness_weight_hl14d'],
            'freshness_adjusted_stance': daily['freshness_adjusted_stance'],
            'days_since_last_scored_speech': daily['days_since_last_scored_speech'],
            'is_score_fresh': daily['is_score_fresh'],
            'has_scored_speech': daily['has_scored_speech'],
            'updated_at': daily['updated_at'],
        })

        bars_src = events[
            (events['analysis_status'] == 'scored') &
            (events['stance_score'].notna())
        ].copy()
        bars = pd.DataFrame({
            'row_id': 'bar:' + bars_src['speech_id'].astype(str),
            'date': bars_src['date'],
            'bank_code': bars_src['bank_code'],
            'mark_type': 'speech_bar',
            'speech_id': bars_src['speech_id'],
            'speaker': bars_src['speaker'],
            'title': bars_src['title'],
            'stance_reason': bars_src['stance_reason'],
            'line_score': pd.NA,
            'bar_score': bars_src['stance_score'],
            'stance_score': bars_src['stance_score'],
            'freshness_weight_hl14d': pd.NA,
            'freshness_adjusted_stance': pd.NA,
            'days_since_last_scored_speech': pd.NA,
            'is_score_fresh': pd.NA,
            'has_scored_speech': 1,
            'updated_at': datetime.now().isoformat(),
        })

        return pd.concat([line, bars], ignore_index=True)

    def upload_sentiment_marts(self):
        """Rebuild and upload Tableau-facing sentiment mart tables."""
        if not self.create_sentiment_mart_tables():
            self.last_mart_counts = {}
            return 0

        events = self.get_sentiment_events_df()
        daily = self.get_sentiment_daily_df()
        plot = self.get_sentiment_plot_df()

        tables = [
            (f"{self.prefix}sentiment_events", events, '"speech_id"', ['speech_id']),
            (f"{self.prefix}sentiment_daily", daily, '"date", "bank_code"', ['date', 'bank_code']),
            (f"{self.prefix}sentiment_plot", plot, '"row_id"', ['row_id']),
        ]

        total = 0
        counts = {}
        for table_name, df, conflict_target, keys in tables:
            self.send_sql(f"DELETE FROM {table_name};")
            count = self.bulk_upsert_df(df, table_name, conflict_target, update_key_cols=keys)
            logger.info(f"  Tableau mart uploaded: {table_name} ({count} rows)")
            counts[table_name.replace(f"{self.prefix}sentiment_", "")] = count
            total += count
        self.last_mart_counts = counts
        return total

    def upload_members(self):
        data = self.db.get_unsynced_members()
        if not data: return 0
        df = pd.DataFrame(data)
        
        # ID 컬럼 정수형 강제 (NaN이 있어도 float 방지)
        if 'id' in df.columns:
            df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')

        # 모든 유효 필드 포함 (Tableau 연결을 위해 id 포함)
        cols = [
            'id', 'bank_code', 'name', 'role', 'status', 'term_start', 'term_end', 
            'last_speech_date', 'last_verified_at', 'avg_stance_score', 'last_updated'
        ]
        df = df[cols]
        count = self.bulk_insert_df(df, f"{self.prefix}members")
        if count:
            self.db.mark_members_as_synced([m['id'] for m in data])
        return count

    def upload_speeches(self, batch_size=100):
        data = self.db.get_unsynced_speeches(limit=batch_size)
        if not data: return 0

        def prepare_df(rows):
            df = pd.DataFrame(rows)

            # ID 컬럼 정수형 강제
            for col in ['id', 'speaker_id']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # m.name as speaker와 s.speaker_id(로컬ID)를 모두 포함하여 연결성 강화
            # 모든 유효 필드 포함 (id 포함)
            cols = [
                'id', 'bank_code', 'speaker', 'speaker_id', 'title', 'date', 'url',
                'full_text', 'speech_type', 'language', 'fetched_at', 'created_at',
                'stance_score', 'stance_reason', 'keywords', 'main_risk'
            ]
            return df[cols]

        df = prepare_df(data)
        count = self.bulk_insert_df(df, f"{self.prefix}speeches")
        if count == len(data):
            self.db.mark_as_synced([s['id'] for s in data])
            return count

        if count:
            logger.warning(
                "Partial speech upload (%s/%s); retrying rows individually before marking synced",
                count,
                len(data),
            )
            count = 0
            for row in data:
                row_count = self.bulk_insert_df(prepare_df([row]), f"{self.prefix}speeches")
                if row_count == 1:
                    self.db.mark_as_synced([row['id']])
                    count += 1
        return count

    def upload_analysis_results(self, batch_size=100):
        data = self.db.get_unsynced_analysis(limit=batch_size)
        if not data: return 0

        def prepare_df(rows):
            df = pd.DataFrame(rows)

            # speech_id 포함 및 정수형 강제
            if 'speech_id' in df.columns:
                df['speech_id'] = pd.to_numeric(df['speech_id'], errors='coerce').astype('Int64')

            # analysis_results 테이블은 url을 기준으로 매칭
            cols = [
                'url', 'speech_id', 'stance_score', 'stance_reason', 'keywords', 'main_risk',
                'analysis_attempts', 'analysis_status', 'analyzed_at'
            ]
            return df[cols]

        df = prepare_df(data)
        count = self.bulk_insert_df(df, f"{self.prefix}analysis_results")
        if count == len(data):
            self.db.mark_analysis_as_synced([d['speech_id'] for d in data])
            return count

        if count:
            logger.warning(
                "Partial analysis upload (%s/%s); retrying rows individually before marking synced",
                count,
                len(data),
            )
            count = 0
            for row in data:
                row_count = self.bulk_insert_df(prepare_df([row]), f"{self.prefix}analysis_results")
                if row_count == 1:
                    self.db.mark_analysis_as_synced([row['speech_id']])
                    count += 1
        return count

    def sync_all(self, batch_size=100):
        """Sync all unsynced data to PostgreSQL in batches."""
        total_m, total_s, total_a, total_marts = 0, 0, 0, 0

        # Members (usually small, single batch is fine)
        m = self.upload_members()
        total_m += m

        # Speeches — batch loop until all synced
        while True:
            s = self.upload_speeches(batch_size=batch_size)
            total_s += s
            if s == 0:
                break
            logger.info(f"  Speeches batch uploaded: {s} (total so far: {total_s})")

        # Analysis results — batch loop
        while True:
            a = self.upload_analysis_results(batch_size=batch_size)
            total_a += a
            if a == 0:
                break
            logger.info(f"  Analysis batch uploaded: {a} (total so far: {total_a})")

        total_marts = self.upload_sentiment_marts()

        self.last_sync_stats = {
            "members": total_m,
            "speeches": total_s,
            "analysis_results": total_a,
            "source_total": total_m + total_s + total_a,
            "tableau_marts": total_marts,
            "mart_counts": dict(self.last_mart_counts),
            "total": total_m + total_s + total_a + total_marts,
        }
        logger.info(
            f"Sync complete — Members: {total_m}, Speeches: {total_s}, "
            f"Analysis: {total_a}, Tableau marts: {total_marts}"
        )
        return self.last_sync_stats["total"]

    def upload_new_speeches(self, limit=None):
        return self.sync_all(batch_size=100)

class DataExporter:
    """Legacy CSV Exporter"""
    def __init__(self, db=None, output_dir=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.output_dir = Path(output_dir) if output_dir else (Path(__file__).parent.parent / "data" / "exports")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(self):
        src = self.db.db_path
        dst = self.output_dir / "speeches.db"
        import shutil
        shutil.copy2(src, dst)
        return [str(dst)]
