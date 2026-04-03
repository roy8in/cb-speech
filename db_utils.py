import os
import sys
import requests
import pandas as pd
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry, TableDefinition,
    SqlType, Inserter, TableName, CreateMode
)

load_dotenv()
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
CERT_PATH = os.getenv("CERT_PATH_ENV", r"C:\Users\infomax\certi\Somansa Root CA.cer")
TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL_ENV", "https://prod-apnortheast-a.online.tableau.com")

# Data Paths
try:
    # .py 파일로 실행할 때
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    # Jupyter Notebook이나 Interactive Window에서 실행할 때
    BASE_DIR = Path.cwd()

DEFAULT_DATA_DIR = BASE_DIR / "3rd party request"
DATA_DIR = os.getenv("DATA_DIR_ENV", str(DEFAULT_DATA_DIR))


# PostgreSQL API Configuration
POSTGRE_API_URL = os.getenv("POSTGRE_API_URL")
POSTGRE_API_KEY = os.getenv("POSTGRE_API_KEY")
POSTGRE_HEADER = {
    "x-api-key": POSTGRE_API_KEY,
    "Content-Type": "application/json"
}

def setup_environment():
    """Sets up environment variables for requests only if the certificate exists."""
    # CERT_PATH가 존재하고, 실제 파일이 있는 경우에만 설정
    if CERT_PATH and os.path.exists(CERT_PATH):
        os.environ['REQUESTS_CA_BUNDLE'] = CERT_PATH
    else:
        # 파일이 없으면 해당 환경 변수를 제거하여 시스템 기본 인증서를 쓰도록 함
        os.environ.pop('REQUESTS_CA_BUNDLE', None)

def connect_tableau() -> Tuple[TSC.Server, TSC.PersonalAccessTokenAuth]:
    token_name = os.getenv("TABLEAU_TOKEN_NAME")
    token_secret = os.getenv("TABLEAU_TOKEN_SECRET")
    site_id = os.getenv("TABLEAU_SITE_ID")

    if not all([token_name, token_secret, site_id]):
        print("[ERROR] Critical Error: Missing Tableau environment variables.")
        sys.exit(1)

    try:
        auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
        server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)
        print("[OK] TSC Auth Object Created")
        return server, auth
    except Exception as e:
        print(f"[ERROR] Error creating Tableau auth object: {e}")
        raise


# -----------------------------------------------------------------------------
# PostgreSQL API Functions (Optimized)
# -----------------------------------------------------------------------------
def send_sql(sql_text):
    if not POSTGRE_API_URL or not POSTGRE_API_KEY:
        print("[WARN] Skipping DB Upload: API URL or Key missing in .env")
        return None
    payload = {"sql": sql_text}
    try:
        response = requests.post(POSTGRE_API_URL, json=payload, headers=POSTGRE_HEADER, verify=False)
        if response.status_code != 200:
            print(f"   [WARN] API returned status {response.status_code}: {response.text[:200]}")
            return None
        if not response.text or not response.text.strip():
            return None
        res_json = response.json()
        if res_json.get("status") == "error":
            raise Exception(f"SQL API Error: {res_json.get('message')}")
        return res_json
    except Exception as e:
        print(f"   [WARN] API Request Failed: {e}")
        return None

def create_table_only(df, table_name):
    """테이블 껍데기만 생성 (인덱스 X) - 속도 향상용"""
    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        col_lower = col_name.lower()
        sql_type = 'TEXT'

        if col_lower == 'date':
            sql_type = 'DATE' # 4 bytes (최소 용량)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = 'TIMESTAMP'
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = 'BOOLEAN' # 1 byte
        elif pd.api.types.is_integer_dtype(dtype):
            if col_lower in ('year', 'trend_score', 'aqi'):
                sql_type = 'SMALLINT'  # 2 bytes (연도, 점수 등 작은 정수)
            else:
                sql_type = 'BIGINT' # 8 bytes (매출, 시가총액 등 큰 정수 대비)
        elif pd.api.types.is_float_dtype(dtype):
            # 비율, 마진, 수익률 같은 데이터는 4-byte 'REAL' 형으로 충분하여 용량을 절반으로 단축
            if any(x in col_lower for x in ['per', 'pbr', 'roe', 'margin', 'ratio', 'chg', 'z_score', 'yield', 'err', 'growth', 'contrib', 'value', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'temp']):
                sql_type = 'REAL' 
            else:
                # 금액, 시총 등 큰 범위의 실수는 8-byte 'DOUBLE PRECISION'
                sql_type = 'DOUBLE PRECISION'
        else:
            sql_type = 'TEXT' # 문자열은 실제 내용 길이만큼만 저장됨 (효율적)
        
        # Meta Tables PK (ibes_code, ds_code, instrument)
        if table_name.startswith("meta_") and col_lower in ['ibes_code', 'ds_code', 'instrument']:
             sql_type += " PRIMARY KEY"

        columns_sql.append(f'"{col_name}" {sql_type}')

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_sql)},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    send_sql(create_sql)
    print(f"   [OK] 테이블 생성 완료 (인덱스 미생성): {table_name}")

def create_indexes(df, table_name):
    """데이터 삽입 후 인덱스 생성"""
    # 1. 날짜 인덱스
    if 'date' in df.columns:
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date" ON {table_name} ("date");')
        
    # 2. 코드 인덱스
    code_col = next((c for c in df.columns if 'code' in c.lower() and 'gics' not in c.lower()), None)
    if code_col:
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{code_col}" ON {table_name} ("{code_col}");')
        
        # 3. 복합 인덱스
        if 'date' in df.columns:
             send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date_{code_col}" ON {table_name} ("date", "{code_col}");')
    
    # 4. IMF 테이블 패턴: country + indicator 복합 인덱스
    if 'country' in df.columns and 'indicator' in df.columns:
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_country" ON {table_name} ("country");')
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_indicator" ON {table_name} ("indicator");')
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_country_indicator" ON {table_name} ("country", "indicator");')

    # 5. Google Trends 테이블 패턴: date + Category, Keyword 인덱스
    if 'Category' in df.columns and 'Keyword' in df.columns:
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_category" ON {table_name} ("Category");')
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_keyword" ON {table_name} ("Keyword");')
        if 'date' in df.columns:
            send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date_category" ON {table_name} ("date", "Category");')

    # 6. AQI/기온 테이블 패턴: city + date 복합 인덱스
    if 'city' in df.columns:
        send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_city" ON {table_name} ("city");')
        if 'date' in df.columns:
            send_sql(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date_city" ON {table_name} ("date", "city");')

    print(f"      [INDEX] 인덱스 생성 완료: {table_name}")

def bulk_insert_df(df, table_name, chunk_size=2000):
    total_rows = len(df)
    import math
    total_chunks = math.ceil(total_rows / chunk_size)
    print(f"   [INSERT] Inserting {total_rows} rows into {table_name} ({total_chunks} chunks)...")

    for idx, start in enumerate(range(0, total_rows, chunk_size), 1):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        values_list = []
        for row in chunk.itertuples(index=False, name=None):
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, str):
                    safe_str = val.replace("'", "''")
                    row_values.append(f"'{safe_str}'")
                elif str(val).lower() in ['true', 'false']: 
                    row_values.append(str(val))
                else: 
                    row_values.append(f"'{val}'")
            values_list.append(f"({', '.join(row_values)})")

        col_names = ', '.join([f'"{c}"' for c in df.columns])
        all_values = ', '.join(values_list)
        
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES {all_values};"
        send_sql(insert_sql)
        print(f"      > Chunk {idx}/{total_chunks} uploaded successfully.")
        
    print(f"   [DONE] DB Upload Complete: {table_name}")

def update_postgresql(df, table_name):
    """Wrapper: Drop -> Create Table -> Insert Data -> Create Index"""
    if df.empty: return
    
    send_sql(f"TRUNCATE TABLE {table_name}")
    # TRUNCATE will fail if the table does not exist, so we ensure it is created first
    create_table_only(df, table_name)
    send_sql(f"TRUNCATE TABLE {table_name}")
    bulk_insert_df(df, table_name, chunk_size=5000) # 속도를 위해 청크 사이즈 증가 (기존 3000 -> 5000)
    create_indexes(df, table_name)

# -----------------------------------------------------------------------------
# Tableau Logic (Hyper API)
# -----------------------------------------------------------------------------
def map_dtype_to_sqltype(dtype) -> SqlType:
    # Force float for ALL numerics to avoid 'an integer is required' when pandas Int64 encounters pd.NA
    if pd.api.types.is_integer_dtype(dtype): return SqlType.double()
    elif pd.api.types.is_float_dtype(dtype): return SqlType.double()
    elif pd.api.types.is_datetime64_any_dtype(dtype): return SqlType.timestamp()
    elif pd.api.types.is_bool_dtype(dtype): return SqlType.bool()
    else: return SqlType.text()

def tableau_update(server, auth, project_target, data_name, data_df, publish_mode="Overwrite", tag_check=False):
    if data_df.empty: return
    project_id = None
    with server.auth.sign_in(auth):
        all_projects, _ = server.projects.get()
        for project in all_projects:
            if project.name == project_target:
                project_id = project.id
                break
        if not project_id:
            print(f"[ERROR] Project '{project_target}' not found.")
            return

    hyper_file_path = f"{data_name}.hyper"
    if os.path.exists(hyper_file_path): os.remove(hyper_file_path)

    # Force cast integer columns to float to match the SqlType.double() Mapping
    # This prevents "Got an invalid value for column, it must be a float instance"
    for col in data_df.columns:
        if pd.api.types.is_integer_dtype(data_df[col].dtype):
            data_df[col] = data_df[col].astype(float)
            
    # Force cast integer columns to float to match the SqlType.double() Mapping
    # This prevents "Got an invalid value for column, it must be a float instance"
    for col in data_df.columns:
        if pd.api.types.is_integer_dtype(data_df[col].dtype):
            data_df[col] = data_df[col].astype(float)
            
    # Create Table Definition using the types BEFORE we convert everything to objects/None
    table_name = TableName(data_name)
    hyper_columns = [TableDefinition.Column(col, map_dtype_to_sqltype(data_df[col].dtype)) for col in data_df.columns]

    # Convert the entire dataframe to objective types and replace NaN/NaT/NA with None
    # so Inserter receives pure Python None instead of float('nan') which causes TypeErrors
    data_df = data_df.astype(object).where(pd.notnull(data_df), None)
            
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, create_mode=CreateMode.CREATE_AND_REPLACE, database=hyper_file_path) as connection:
            table_def = TableDefinition(table_name=table_name, columns=hyper_columns)
            connection.catalog.create_table(table_def)
            with Inserter(connection, table_def) as inserter:
                for row in data_df.itertuples(index=False, name=None):
                    row_data = []
                    for i, val in enumerate(row):
                        # Convert numpy types to native Python types first
                        if hasattr(val, 'item'):
                            val = val.item()
                            
                        if pd.isna(val):
                            row_data.append(None)
                        else:
                            expected_type = hyper_columns[i].type
                            if expected_type == SqlType.double():
                                row_data.append(float(val))
                            elif expected_type == SqlType.big_int():
                                row_data.append(int(val))
                            elif expected_type == SqlType.bool():
                                row_data.append(bool(val))
                            elif expected_type == SqlType.text():
                                row_data.append(str(val))
                            else:
                                row_data.append(val)
                    inserter.add_row(row_data)
                inserter.execute()

    with server.auth.sign_in(auth):
        new_datasource = TSC.DatasourceItem(project_id=project_id, name=data_name)
        server.datasources.publish(new_datasource, hyper_file_path, mode=publish_mode)

    print(f"[DONE] {data_name}.hyper Upload Complete")
    if os.path.exists(hyper_file_path): os.remove(hyper_file_path)

# -----------------------------------------------------------------------------
# Wrappers for Main
# -----------------------------------------------------------------------------

def upload_data(server, auth, project_name, data_name, df):
    """Tableau(전체) + DB(경량화) 통합 업로드 (레거시 지원용)"""
    print(f"[RUN] Processing Upload: {data_name}")
    try:
        tableau_update(server, auth, project_name, data_name, df, "Overwrite", False)
    except Exception as e:
        print(f"   [WARN] Tableau Upload Failed: {e}")
    try:
        upload_fact_data(df, data_name.lower())
    except Exception as e:
        print(f"   [WARN] DB Upload Failed: {e}")

def upload_metadata_master(df, table_name):
    """메타데이터 테이블 업로드 (Truncate -> Insert)"""
    print(f"[RUN] Uploading Master Metadata: {table_name}")
    
    # 메타데이터용 테이블 생성 (Primary Key 포함)
    columns_sql = []
    for col in df.columns:
        if col.lower() in ['ibes_code', 'ds_code', 'instrument']:
            columns_sql.append(f'"{col}" TEXT PRIMARY KEY')
        else:
            columns_sql.append(f'"{col}" TEXT')
            
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)}, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
    send_sql(create_sql)
    
    send_sql(f"TRUNCATE TABLE {table_name}")
    bulk_insert_df(df, table_name, chunk_size=15000) # 메타데이터는 컬럼이 적고 가벼우므로 15000건씩 한 번에 전송
    print(f"   [OK] Metadata Updated: {len(df)} rows")

def upload_fact_data(df, table_name):
    """Fact 데이터 업로드 (메타 컬럼 제거 -> update_postgresql)"""
    if table_name.lower() == 'eci_dy':
        # eci_dy는 별도 메타데이터 테이블이 없으므로 메타 컬럼(종목명 등)을 제거하지 않고 그대로 업로드
        df_slim = df
    else:
        meta_cols = ['name', 'gics_code', 'index_group', 'TickerFull'] 
        cols_to_drop = [c for c in meta_cols if c in df.columns]
        df_slim = df.drop(columns=cols_to_drop)
    
    print(f"[RUN] Uploading Fact Data: {table_name}")
    update_postgresql(df_slim, table_name)