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

    def send_sql(self, sql_text):
        """db_utils.py 호환 SQL 전송 함수"""
        if not self.api_url or not self.api_key:
            logger.warning("PostgreSQL API URL or Key missing in .env")
            return None
        
        # API가 'sql' 필드를 기대함 (기존 'query' 아님)
        payload = {"sql": sql_text}
        try:
            # verify=False는 db_utils.py 설정을 따름
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
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP'
            elif pd.api.types.is_integer_dtype(dtype):
                sql_type = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = 'DOUBLE PRECISION'
            
            # Primary Key 설정 (url 또는 bank_code+name)
            pk = ""
            if col_lower == 'url':
                pk = " PRIMARY KEY"
            
            columns_sql.append(f'"{col_name}" {sql_type}{pk}')

        # 복합 키 처리 (members 테이블용)
        extra_constraints = ""
        if table_name.endswith("members"):
            extra_constraints = f', PRIMARY KEY ("bank_code", "name")'
            # 개별 컬럼에서 PK 제거 (이미 추가된 경우 대비)
            columns_sql = [c.replace(" PRIMARY KEY", "") for c in columns_sql]

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
            conflict_target = '"bank_code", "name"'
        
        update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c.lower() not in ['url', 'bank_code', 'name']]

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

    def upload_members(self):
        data = self.db.get_unsynced_members()
        if not data: return 0
        df = pd.DataFrame(data)
        # 필요 없는 컬럼 제거 및 정리
        cols = ['bank_code', 'name', 'role', 'status', 'term_start', 'term_end', 'last_speech_date', 'avg_stance_score', 'last_updated']
        df = df[cols]
        count = self.bulk_insert_df(df, f"{self.prefix}members")
        if count:
            self.db.mark_members_as_synced([m['id'] for m in data])
        return count

    def upload_speeches(self, batch_size=100):
        data = self.db.get_unsynced_speeches(limit=batch_size)
        if not data: return 0
        df = pd.DataFrame(data)
        cols = ['bank_code', 'speaker', 'title', 'date', 'url', 'full_text', 'speech_type', 'language', 'fetched_at']
        df = df[cols]
        count = self.bulk_insert_df(df, f"{self.prefix}speeches")
        if count:
            self.db.mark_as_synced([s['id'] for s in data])
        return count

    def upload_analysis_results(self, batch_size=100):
        data = self.db.get_unsynced_analysis(limit=batch_size)
        if not data: return 0
        df = pd.DataFrame(data)
        # analysis_results 테이블은 url을 기준으로 매칭
        cols = ['url', 'stance_score', 'stance_reason', 'keywords', 'main_risk', 'analysis_status', 'analyzed_at']
        df = df[cols]
        count = self.bulk_insert_df(df, f"{self.prefix}analysis_results")
        if count:
            self.db.mark_analysis_as_synced([d['speech_id'] for d in data])
        return count

    def sync_all(self, batch_size=100):
        """Sync all unsynced data to PostgreSQL in batches."""
        total_m, total_s, total_a = 0, 0, 0

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

        logger.info(f"Sync complete — Members: {total_m}, Speeches: {total_s}, Analysis: {total_a}")
        return total_m + total_s + total_a

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
