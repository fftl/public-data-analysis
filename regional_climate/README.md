# regional_climate 프로젝트

public_data 프로젝트의 일부로 지역별 날씨 데이터를 수집하고 분석합니다. 기상청에서 제공하는 종관기상관측(ASOS) 데이터를 기반으로 **전국 17개 시도별 기후 트렌드, 지역 간 비교, 이상기후 추이**를 분석할 수 있는 Fact 테이블을 구축합니다.

## 기술 스택
수집 및 가공 : **Python, Pandas, Jupyter Lab**
데이터베이스 : **MySQL, BigQuery**
파이프라인 : **Airflow**
시각화 : **Looker Studio**
프로젝트 관리 : **Github**

## 데이터 소스
사용 데이터는 [**기상자료개방포털**](https://data.kma.go.kr/cmmn/main.do)에서 제공하고 있는 [**종관기상관측(ASOS)**](https://data.kma.go.kr/data/grnd/selectAsosRltmList.do?pgmNo=36)의 데이터를 2000년 1월 ~ 2026년 2월까지의 데이터를 수집하였습니다.

#### 제공 데이터
전국 100여 개 관측소에서 수집된 월별 기상 데이터로, 주요 항목은 다음과 같습니다.

| 분류 | 항목 |
|------|------|
| 기온 | 평균기온, 최고기온, 최저기온, 일 최고/최저 극값 |
| 기압/습도 | 해면기압, 증기압, 이슬점온도, 상대습도 |
| 강수 | 월 강수량 합계, 일 최다강수량, 강수량 표준편차 |
| 바람 | 평균풍속, 최대풍속, 최다풍향 |
| 일조/지면 | 합계일조시간, 합계일사량, 평균지면온도 |
| 적설 | 최심적설 |

## 파이프라인 아키텍처

```
CSV (기상청) → MySQL (Raw) → BigQuery (Raw/Meta) → BigQuery (Silver) → BigQuery (Fact) → Looker Studio
```

## 파이프라인 상세

### 1. regional_climate_raw.ipynb (Raw)
기상청에서 다운로드한 CSV 파일을 MySQL에 적재하는 단계입니다.

- `regional_climate_raw.csv` (관측 데이터, 27,668건) 및 `regional_climate_meta.csv` (관측소 메타, 140건) 로드
- EUC-KR 인코딩 처리
- MySQL 테이블 `regional_climate`, `regional_climate_meta`에 저장

### 2. regional_climate_transform.ipynb (Transform)
MySQL의 RAW 데이터를 정제하여 BigQuery에 적재합니다.

- **관측소 정리**: 2000년 이전 폐쇄 관측소, 단일 관측 데이터만 있는 관측소, 산지 관측소 제거
- **결측 지역 보정**: 누락된 관측소 위치 정보 수동 매핑 (예: "서청주(예)" → "충청북도 청주시")
- **컬럼명 표준화**: 특수문자 및 단위 기호 제거 (예: `평균기온(°C)` → `평균기온_C`)
- **데이터 타입 변환**: 수치형 컬럼 float 변환, 관측소 ID 정수 변환
- BigQuery `raw` 및 `meta` 테이블에 저장

### 3. regional_climate_make_silver.ipynb (Silver)
69개 원시 컬럼 중 분석에 필요한 **21개 핵심 기후 지표**를 선별하고, 관측소를 시도 단위로 매핑합니다.

- 날짜 컬럼 파싱 → year, month 추출
- 관측소 주소 기반으로 17개 시도(sd_name)에 매핑
- 공통 참조 테이블(`sd_name_alias`, `sd_name_std`)을 활용한 시도명 표준화
- 최종 Silver 데이터: **27,032건**

### 4. regional_climate_make_fact.ipynb (Fact)
Silver 데이터를 집계하여 3개의 분석용 Fact 테이블을 생성합니다.

#### fact_climate_trend (440건)
시도 × 연도 단위의 기후 변화 추이 테이블입니다.
- 평균/최고/최저 기온, 연 극값, 강수량 합계, 습도, 기압, 일조시간, 지면온도
- 일교차(최고기온 - 최저기온) 계산
- 평균기온 전년 대비 변화량(YoY) 계산
- 불완전 연도(12개월 미만) 제외

#### fact_regional_compare (204건)
시도 × 월 단위의 기후 정상값(climatological normals) 테이블입니다.
- 기온(평균, 최고, 최저, 일교차), 강수(평균, 표준편차), 습도, 풍속, 운량, 일조시간, 일사량, 지면온도
- 전 기간 평균으로 월별 기후 패턴 파악에 활용

#### fact_extreme_weather (440건)
시도 × 연도 단위의 이상기후 추적 테이블입니다.
- 연 최고/최저 기온 극값 및 기온 범위
- 일 최다강수량, 월 최다강수량, 연 평균 총 강수량
- 최대풍속, 최심적설
- 일 최다강수량/최고기온 전년 대비 변화량(YoY) 계산

## Fact 데이터 모델링

### fact_climate_trend
| 컬럼 | 타입 | 설명 |
|------|------|------|
| sd_code | INT64 | 시도 코드 |
| sd_name_std | STRING | 시도명 (표준) |
| year | INT64 | 연도 |
| avg_temp | FLOAT64 | 평균기온 (°C) |
| max_temp | FLOAT64 | 평균 최고기온 (°C) |
| min_temp | FLOAT64 | 평균 최저기온 (°C) |
| highest_temp | FLOAT64 | 연 최고기온 극값 (°C) |
| lowest_temp | FLOAT64 | 연 최저기온 극값 (°C) |
| daily_temp_range | FLOAT64 | 일교차 (°C) |
| total_precip_mm | FLOAT64 | 연 강수량 합계 (mm) |
| avg_humidity | FLOAT64 | 평균 상대습도 (%) |
| avg_pressure | FLOAT64 | 평균 해면기압 (hPa) |
| sunshine_hours | FLOAT64 | 합계 일조시간 (hr) |
| ground_temp | FLOAT64 | 평균 지면온도 (°C) |
| avg_temp_yoy_diff | FLOAT64 | 평균기온 전년 대비 차이 (°C) |

### fact_regional_compare
| 컬럼 | 타입 | 설명 |
|------|------|------|
| sd_code | INT64 | 시도 코드 |
| sd_name_std | STRING | 시도명 (표준) |
| month | INT64 | 월 |
| avg_temp | FLOAT64 | 평균기온 (°C) |
| max_temp | FLOAT64 | 평균 최고기온 (°C) |
| min_temp | FLOAT64 | 평균 최저기온 (°C) |
| daily_temp_range | FLOAT64 | 일교차 (°C) |
| avg_precip | FLOAT64 | 평균 강수량 (mm) |
| precip_std | FLOAT64 | 강수량 표준편차 (mm) |
| avg_humidity | FLOAT64 | 평균 상대습도 (%) |
| avg_wind_speed | FLOAT64 | 평균 풍속 (m/s) |
| avg_cloud | FLOAT64 | 평균 운량 |
| sunshine_hours | FLOAT64 | 합계 일조시간 (hr) |
| solar_radiation | FLOAT64 | 합계 일사량 (MJ/m²) |
| ground_temp | FLOAT64 | 평균 지면온도 (°C) |

### fact_extreme_weather
| 컬럼 | 타입 | 설명 |
|------|------|------|
| sd_code | INT64 | 시도 코드 |
| sd_name_std | STRING | 시도명 (표준) |
| year | INT64 | 연도 |
| highest_temp | FLOAT64 | 연 최고기온 (°C) |
| lowest_temp | FLOAT64 | 연 최저기온 (°C) |
| temp_range | FLOAT64 | 연 기온 범위 (°C) |
| max_daily_precip | FLOAT64 | 일 최다강수량 (mm) |
| max_monthly_precip | FLOAT64 | 월 최다강수량 (mm) |
| avg_total_precip | FLOAT64 | 연 평균 총 강수량 (mm) |
| max_wind_speed | FLOAT64 | 최대풍속 (m/s) |
| max_snow_depth | FLOAT64 | 최심적설 (cm) |
| max_daily_precip_yoy | FLOAT64 | 일 최다강수량 YoY 차이 (mm) |
| highest_temp_yoy | FLOAT64 | 최고기온 YoY 차이 (°C) |

## 작업 과정
- 69개 컬럼 중 21개를 선별한 기준은?
- 관측소를 시도 단위로 매핑하는 방법은?
- 불완전 연도를 제외하는 이유는?
