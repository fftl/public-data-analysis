# =============================================================================
# regional_climate Fact 테이블 생성 스크립트
# silver(raw에서 21컬럼 선별) → 시도 매핑 → 3개 fact 테이블 적재
# =============================================================================

# %% [1] 환경 설정
import pymysql
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import re

load_dotenv()

# %% [2] 연결 설정
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-credentials.json'
project_id = os.getenv('GCP_PROJECT_ID')
dataset = os.getenv('BQ_DATASET_RC')  # regional_climate

big_engine = create_engine(f'bigquery://{project_id}/{dataset}')

DB_URL = f"mysql+pymysql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
my_engine = create_engine(DB_URL)

client = bigquery.Client(project=project_id)

# %% [3] raw 데이터 가져오기 (BigQuery)
query = text("""
    select *
    from regional_climate.raw
""")

with big_engine.connect() as conn:
    result = conn.execute(query)
    df_raw = pd.DataFrame(result.fetchall(), columns=result.keys())

print(f'raw 데이터: {len(df_raw)}행, {len(df_raw.columns)}컬럼')

# %% [4] silver 컬럼 선별 (21개)
# BigQuery에 적재 시 clean_column_name으로 변환된 이름 사용
silver_columns = [
    '지점', '지점명', '일시',
    '평균기온_C', '평균최고기온_C', '평균최저기온_C',
    '최고기온_C', '최저기온_C',
    '평균해면기압_hPa',
    '평균수증기압_hPa',
    '평균이슬점온도_C',
    '평균상대습도',
    '월합강수량_00_24h만_mm',
    '일최다강수량_mm',
    '평균풍속_m_s', '최대풍속_m_s',
    '최다풍향_16방위',
    '평균운량_1_10',
    '합계_일조시간_hr',
    '합계_일사량_MJ_m2',
    '최심적설_cm',
    '평균지면온도_C',
]

df = df_raw[silver_columns].copy()
print(f'silver 컬럼 선별 완료: {len(df.columns)}컬럼')

# %% [5] 숫자 타입 변환
numeric_cols = [c for c in df.columns if c not in ['지점', '지점명', '일시', '최다풍향_16방위']]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['지점'] = df['지점'].astype(int)

# %% [6] 일시 파싱 → year, month
df['year'] = df['일시'].str[:4].astype(int)
df['month'] = df['일시'].str[5:7].astype(int)

# %% [7] 관측소 메타데이터 가져오기 (MySQL)
# ※ 지점주소가 필요 → raw 테이블이 아닌 메타 테이블에서 가져옴
query_meta = text("""
    select 지점, 지점명, 지점주소
    from regional_climate_meta
""")

with my_engine.connect() as conn:
    result = conn.execute(query_meta)
    df_meta = pd.DataFrame(result.fetchall(), columns=result.keys())

# 메타 전처리 (transform에서 했던 것과 동일)
df_meta.loc[df_meta['지점명'] == '서청주(예)', '지점주소'] = '충청북도 청주시'
df_meta = df_meta[~df_meta['지점주소'].str.contains('(산지)', na=False, regex=False)]
df_meta = df_meta.drop_duplicates(subset='지점', keep='last')

print(f'관측소 메타: {len(df_meta)}개 지점')

# %% [8] 지점주소에서 시도명 추출
df_meta['sd_raw'] = df_meta['지점주소'].str.split().str[0]

# %% [9] sd_name_alias, sd_name_std 가져오기 (BigQuery common)
query_alias = text(f"""
    select * from `{project_id}.common.sd_name_alias`
""")
query_std = text(f"""
    select * from `{project_id}.common.sd_name_std`
""")

with big_engine.connect() as conn:
    df_alias = pd.DataFrame(conn.execute(query_alias).fetchall(),
                            columns=conn.execute(query_alias).keys())

with big_engine.connect() as conn:
    df_std = pd.DataFrame(conn.execute(query_std).fetchall(),
                          columns=conn.execute(query_std).keys())

print(f'alias: {len(df_alias)}행, std: {len(df_std)}행')

# %% [10] 관측소 → sd_code 매핑
# df_meta.sd_raw → sd_name_alias JOIN → sd_code
df_meta = df_meta.merge(
    df_alias,
    left_on='sd_raw',
    right_on='sd_name_alias',
    how='left'
)

# 매핑 확인
missing = df_meta[df_meta['sd_code'].isna()]
if len(missing) > 0:
    print(f'⚠ 시도 매핑 실패 {len(missing)}건:')
    print(missing[['지점', '지점명', 'sd_raw']].to_string())

# %% [11] 기후 데이터에 시도 정보 붙이기
# df_meta에서 지점 → sd_code 매핑만 추출
station_sd = df_meta[['지점', 'sd_code']].dropna(subset=['sd_code'])
station_sd['sd_code'] = station_sd['sd_code'].astype(int)

df = df.merge(station_sd, on='지점', how='inner')

# sd_code → sd_name_std 붙이기
df = df.merge(
    df_std.rename(columns={'sd_code': 'sd_code', 'sd_name_std': 'sd_name_std'}),
    on='sd_code',
    how='left'
)

df = df.rename(columns={'sd_code': 'sd_code'})
print(f'시도 매핑 완료: {len(df)}행, 시도 {df["sd_name_std"].nunique()}개')


# =============================================================================
# FACT 1: fact_climate_trend (시도 × 연도)
# =============================================================================
# %% [12] fact_climate_trend 생성

# Step 1: 관측소별 연간 집계
station_year = df.groupby(['지점', 'sd_code', 'sd_name_std', 'year']).agg(
    avg_temp=('평균기온_C', 'mean'),
    avg_max_temp=('평균최고기온_C', 'mean'),
    avg_min_temp=('평균최저기온_C', 'mean'),
    year_max_temp=('최고기온_C', 'max'),
    year_min_temp=('최저기온_C', 'min'),
    total_precip_mm=('월합강수량_00_24h만_mm', 'sum'),
    avg_humidity_pct=('평균상대습도', 'mean'),
    avg_pressure_hpa=('평균해면기압_hPa', 'mean'),
    total_sunshine_hr=('합계_일조시간_hr', 'sum'),
    avg_ground_temp=('평균지면온도_C', 'mean'),
    avg_max_temp_raw=('평균최고기온_C', 'mean'),
    avg_min_temp_raw=('평균최저기온_C', 'mean'),
).reset_index()

# 관측소별 일교차
station_year['avg_temp_range'] = station_year['avg_max_temp_raw'] - station_year['avg_min_temp_raw']
station_year = station_year.drop(columns=['avg_max_temp_raw', 'avg_min_temp_raw'])

# Step 2: 시도별 평균 (관측소 수 편향 제거)
fact_trend = station_year.groupby(['sd_code', 'sd_name_std', 'year']).agg(
    avg_temp=('avg_temp', 'mean'),
    avg_max_temp=('avg_max_temp', 'mean'),
    avg_min_temp=('avg_min_temp', 'mean'),
    year_max_temp=('year_max_temp', 'max'),
    year_min_temp=('year_min_temp', 'min'),
    total_precip_mm=('total_precip_mm', 'mean'),
    avg_humidity_pct=('avg_humidity_pct', 'mean'),
    avg_pressure_hpa=('avg_pressure_hpa', 'mean'),
    total_sunshine_hr=('total_sunshine_hr', 'mean'),
    avg_ground_temp=('avg_ground_temp', 'mean'),
    avg_temp_range=('avg_temp_range', 'mean'),
).reset_index()

# Step 3: 전년 대비 기온 변화
fact_trend = fact_trend.sort_values(['sd_code', 'year'])
fact_trend['temp_yoy_diff'] = fact_trend.groupby('sd_code')['avg_temp'].diff()

# 소수점 정리
float_cols = fact_trend.select_dtypes(include='float64').columns
fact_trend[float_cols] = fact_trend[float_cols].round(2)

print(f'fact_climate_trend: {len(fact_trend)}행')
print(fact_trend.head())


# =============================================================================
# FACT 2: fact_regional_compare (시도 × 월, 전체 연도 평년값)
# =============================================================================
# %% [13] fact_regional_compare 생성

# 월별 일교차 계산용 컬럼 추가
df['daily_range'] = df['평균최고기온_C'] - df['평균최저기온_C']

# Step 1: 관측소별 월-연도 값은 이미 df에 있음. 관측소별 월 평년값
station_month = df.groupby(['지점', 'sd_code', 'sd_name_std', 'month']).agg(
    avg_temp=('평균기온_C', 'mean'),
    avg_max_temp=('평균최고기온_C', 'mean'),
    avg_min_temp=('평균최저기온_C', 'mean'),
    avg_daily_range=('daily_range', 'mean'),
    avg_precip_mm=('월합강수량_00_24h만_mm', 'mean'),
    precip_stddev=('월합강수량_00_24h만_mm', 'std'),
    avg_humidity_pct=('평균상대습도', 'mean'),
    avg_wind_speed=('평균풍속_m_s', 'mean'),
    avg_cloud_cover=('평균운량_1_10', 'mean'),
    avg_sunshine_hr=('합계_일조시간_hr', 'mean'),
    avg_solar_mj=('합계_일사량_MJ_m2', 'mean'),
    avg_ground_temp=('평균지면온도_C', 'mean'),
).reset_index()

# Step 2: 시도별 평균
fact_compare = station_month.groupby(['sd_code', 'sd_name_std', 'month']).agg(
    avg_temp=('avg_temp', 'mean'),
    avg_max_temp=('avg_max_temp', 'mean'),
    avg_min_temp=('avg_min_temp', 'mean'),
    avg_daily_range=('avg_daily_range', 'mean'),
    avg_precip_mm=('avg_precip_mm', 'mean'),
    precip_stddev=('precip_stddev', 'mean'),
    avg_humidity_pct=('avg_humidity_pct', 'mean'),
    avg_wind_speed=('avg_wind_speed', 'mean'),
    avg_cloud_cover=('avg_cloud_cover', 'mean'),
    avg_sunshine_hr=('avg_sunshine_hr', 'mean'),
    avg_solar_mj=('avg_solar_mj', 'mean'),
    avg_ground_temp=('avg_ground_temp', 'mean'),
).reset_index()

float_cols = fact_compare.select_dtypes(include='float64').columns
fact_compare[float_cols] = fact_compare[float_cols].round(2)

print(f'fact_regional_compare: {len(fact_compare)}행')
print(fact_compare.head())


# =============================================================================
# FACT 3: fact_extreme_weather (시도 × 연도)
# =============================================================================
# %% [14] fact_extreme_weather 생성

# Step 1: 관측소별 연간 극단값
station_extreme = df.groupby(['지점', 'sd_code', 'sd_name_std', 'year']).agg(
    year_max_temp=('최고기온_C', 'max'),
    year_min_temp=('최저기온_C', 'min'),
    max_daily_precip=('일최다강수량_mm', 'max'),
    max_monthly_precip=('월합강수량_00_24h만_mm', 'max'),
    total_precip_mm=('월합강수량_00_24h만_mm', 'sum'),
    max_wind_speed=('최대풍속_m_s', 'max'),
    max_snowfall_cm=('최심적설_cm', 'max'),
).reset_index()

station_extreme['year_temp_range'] = station_extreme['year_max_temp'] - station_extreme['year_min_temp']

# Step 2: 시도별 집계 (극단값은 max, 강수총량은 mean)
fact_extreme = station_extreme.groupby(['sd_code', 'sd_name_std', 'year']).agg(
    year_max_temp=('year_max_temp', 'max'),
    year_min_temp=('year_min_temp', 'min'),
    year_temp_range=('year_temp_range', 'max'),
    max_daily_precip=('max_daily_precip', 'max'),
    max_monthly_precip=('max_monthly_precip', 'max'),
    total_precip_mm=('total_precip_mm', 'mean'),
    max_wind_speed=('max_wind_speed', 'max'),
    max_snowfall_cm=('max_snowfall_cm', 'max'),
).reset_index()

# Step 3: 전년 대비 변화
fact_extreme = fact_extreme.sort_values(['sd_code', 'year'])
fact_extreme['max_daily_precip_yoy_diff'] = fact_extreme.groupby('sd_code')['max_daily_precip'].diff()
fact_extreme['max_temp_yoy_diff'] = fact_extreme.groupby('sd_code')['year_max_temp'].diff()

float_cols = fact_extreme.select_dtypes(include='float64').columns
fact_extreme[float_cols] = fact_extreme[float_cols].round(2)

print(f'fact_extreme_weather: {len(fact_extreme)}행')
print(fact_extreme.head())


# =============================================================================
# BigQuery 적재
# =============================================================================
# %% [15] BigQuery 적재

def load_to_bq(df, table_name):
    table_id = f'{project_id}.{dataset}.{table_name}'
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f'적재 완료: {table_name} → {job.output_rows}행')

load_to_bq(fact_trend, 'fact_climate_trend')
load_to_bq(fact_compare, 'fact_regional_compare')
load_to_bq(fact_extreme, 'fact_extreme_weather')

print('전체 적재 완료')