# HOUSEHOLD_POWER 프로젝트

KEPCO 공공 API에서 지역별 가구 평균 전력 사용량 데이터를 수집하고, 정제 및 집계를 거쳐 분석용 Fact 테이블을 구축한 프로젝트입니다.

전력 사용량은 날씨, 인구 등 다양한 외부 요인과 결합하여 분석하기 좋은 소재라는 점과, 수집 데이터 내에 지역별 가구 수 정보가 포함되어 있어 다양하게 활용할 수 있다는 점에서 첫 번째 주제로 선정하였습니다.

---

## 기술 스택

| 역할 | 기술 |
|---|---|
| 수집 및 가공 | Python, Pandas, Jupyter Lab |
| 데이터베이스 | MySQL (Bronze), BigQuery (Silver/Gold) |
| 파이프라인 자동화 | Airflow |
| 시각화 | Looker Studio |
| 프로젝트 관리 | GitHub |

---

## 데이터 소스

- **출처**: [전력데이터 개방 포털시스템](https://bigdata.kepco.co.kr/cmsmain.do?scode=S01&pcode=main&pstate=L&redirect=Y) — [가구평균 전력사용량 API](https://bigdata.kepco.co.kr/cmsmain.do?scode=S01&pcode=000493&pstate=house&redirect=Y)
- **수집 범위**: 2013년 5월 ~ 2025년 12월 / 전국 17개 광역시도 (시군구 단위)

| 필드 | 설명 |
|---|---|
| year, month | 연도, 월 |
| metro (sd_name) | 광역시도 |
| city (sgg_name) | 시군구 |
| houseCnt | 해당 지역 가구 수 |
| powerUsage | 가구 평균 전력 사용량 (kWh) |
| bill | 가구 평균 전기 요금 (원) |

---

## 파이프라인 아키텍처

<img src="docs/household_power.drawio.png" width="600" height="300"/>

```
KEPCO API → MySQL (Bronze) → BigQuery Silver (raw) → BigQuery Gold (fact) → Looker Studio
```

---

## 파이프라인 단계별 설명

### 1단계: 데이터 수집 — Bronze (`scripts/household_power_insert.ipynb`)

KEPCO API에서 수집한 데이터를 **가공 없이 원본 그대로** MySQL에 저장합니다.

- 17개 광역시도 × 연도(2013~2025) × 월(1~12) 순회 수집
- `is_already_collected()`로 중복 수집 방지 — 중단 후 재개 가능
- API 요청 간 5초 대기로 Rate Limit 준수
- `save_batch()`로 배치 단위 MySQL 저장

> 원본 데이터를 별도 보존하는 이유: 가공 과정에서 문제가 발생했을 때, 파이프라인 버그인지 원본 데이터 자체의 문제인지 구분하기 위함입니다.

→ 상세 내용: [docs/01_데이터_수집.md](docs/01_데이터_수집.md)

---

### 2단계: 데이터 변환 — Silver (`scripts/household_power_transform.ipynb`)

MySQL RAW 데이터를 읽어 품질 문제를 처리한 후 BigQuery Silver 테이블에 적재합니다.

- **음수/이상치 처리**: 물리적으로 불가능한 음수 값 → NaN 변환
- **결측치 보간**: 지역별 그룹 내 선형 보간(interpolation)으로 채우기
- **행정구역 통합**: 구역 개편으로 분리된 지역을 가구 수 가중평균으로 병합
  (예: 부천시 원미구/소사구/오정구 → 부천시)

→ 상세 내용: [docs/02_데이터_변환.md](docs/02_데이터_변환.md)

---

### 3단계: Fact 테이블 생성 — Gold (`scripts/household_power_make_fact.ipynb`)

Silver 데이터에 시간 차원과 파생 지표를 추가하여 분석용 Fact 테이블을 생성합니다.

**추가 차원**

| 컬럼 | 설명 |
|---|---|
| quarter | 분기 |
| season | 계절 (봄/여름/가을/겨울) |
| season_tariff | 한전 요금제 계절 구분 (하계 7~8월 / 기타) |

**파생 지표**

| 컬럼 | 설명 |
|---|---|
| total_usage_kwh | 지역 전체 소비량 (가구수 × 평균사용량) |
| total_bill_won | 지역 전체 요금 (가구수 × 평균요금) |
| unit_price | kWh당 단가 (평균요금 / 평균사용량) |
| usage_yoy_pct | 전년 동월 대비 변화율 |
| usage_mom_pct | 전월 대비 변화율 |

→ 상세 내용: [docs/03_팩트테이블_생성.md](docs/03_팩트테이블_생성.md)

---

### 4단계: 수집 자동화 (Airflow)

매주 API를 확인하여 신규 데이터가 추가된 경우 자동으로 수집하는 DAG를 구성합니다.

- DB 최신 연월 기준으로 다음 달 신규 데이터 존재 여부 확인
- 실패한 지역만 모아 최대 10회 재시도
- 재시도 후에도 실패한 항목은 파일로 기록하여 수동 처리

→ 상세 내용: [docs/04_수집_자동화.md](docs/04_수집_자동화.md)

---

## Fact 테이블 스키마

```sql
CREATE TABLE household_power.fact (
    year              INT64,
    month             INT64,
    quarter           INT64,
    season            STRING,
    season_tariff     STRING,
    sd_code           INT64,
    sd_name           STRING,
    sgg_name          STRING,
    house_cnt         INT64,
    avg_usage_kwh     FLOAT64,
    avg_bill_won      FLOAT64,
    total_usage_kwh   FLOAT64,
    total_bill_won    FLOAT64,
    unit_price        FLOAT64,
    usage_yoy_pct     FLOAT64,
    usage_mom_pct     FLOAT64
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2016, 2030, 1))
CLUSTER BY sd_name, sgg_name;
```

---

## 시각화

Looker Studio 대시보드: https://lookerstudio.google.com/reporting/c2722fff-023f-4023-a24c-c67660254b12
