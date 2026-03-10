# HOUSEHOLD_POWER 프로젝트

public_data 프로젝트의 일부로 지역별 가구 평균 전력 사용량 데이터를 수집하고 분석합니다. 지역별, 기간별 데이터를 제공하는 공공데이터 중 **전력 사용량의 경우 날씨, 주거 인구수 등과 결합하여 분석하기 좋은 소재라고 생각되어** 가장 먼저 진행하게 되었습니다.

## 기술 스택
수집 및 가공 : **Python, Pandas, Jupyter Lab**  
데이터베이스 : **MySQL, BigQuery**  
파이프라인 : **Airflow**  
시각화 : **Looker Studio**  
프로젝트 관리 : **Github**

## 데이터 소스
사용 데이터는 [**전력데이터 개방 포털시스템**](https://bigdata.kepco.co.kr/cmsmain.do?scode=S01&pcode=main&pstate=L&redirect=Y)에서 제공하고 있는 [**가구평균 전력사용량 API**](https://bigdata.kepco.co.kr/cmsmain.do?scode=S01&pcode=000493&pstate=house&redirect=Y)를 수집하여 사용하였습니다.

#### 제공 데이터
API에서는 다음과 같은 데이터를 제공하고 있습니다.

<img src="docs/image.png" width="200" height="200"/>

**year, month, metro, city**는 각각 일자와 지역,  
**houseCnt**는 지역의 가구 수,  
**powerUsage**는 지역의 평균 전력 사용량,  
**bill**은 평균 전기 요금을 나타냅니다.


데이터는 2015년 5월부터 2025년 12월까지의 데이터를 수집하였고, 주에 한번씩 API를 확인하여 새로운 데이터가 나왔을 경우 자동으로 수집되도록 Airflow 파이프라인을 구축하였습니다.

## 파이프라인 아키텍처
<img src="docs/household_power.drawio.png" width="600" height="300"/>

## 파이프라인 상세
- insert.ipynb
  - 최초로 API를 이용해 데이터를 수집하는 과정입니다. **수집한 그대로의 순수한 RAW데이터를 MySQL에 저장**합니다. (Bronze Data)
- transform.ipynb
  - MySQL의 RAW데이터를 읽어 Pandas를 이용해 데이터를 클렌징합니다. **결측치를 보간법을 통해서 수정**하고, **변형된 지역의 이름을 통합**해주는 작업을 통해 데이터를 다듬고, **BigQuery에 가공된 데이터를 저장**합니다. (Silver Data)
- make_fact.ipynb
  - Silver Data를 가지고 Fact데이터를 만듭니다. **월별 기록을 통해 계절을 나누고, 전월대비 전력량 상승률 등을 계산**합니다. 또한 **평균치를 가구수와 곱하여 실제 총 사용량을 계산**하기도 하였습니다. (Gold Data)

## Fact 데이터 모델링
```mysql
CREATE TABLE household_power.fact(
	-- 차원 (Dimension) --
    year              INT64,
    month             INT64,
    quarter           INT64,            -- 분기
    season            STRING,           -- 계절
    season_tariff 	  STRING,			-- 요금제 계절 구분
    
    sd_code           INT64,
    sd_name           STRING,
    sgg_name          STRING,   
    
    house_cnt         INT64,
    avg_usage_kwh     FLOAT64,           -- power_usage 리네이밍
    avg_bill_won      FLOAT64,             -- bill 리네이밍
    
    total_usage_kwh   FLOAT64,           -- house_cnt * avg_usage_kwh
    total_bill_won    FLOAT64,           -- house_cnt * avg_bill_won
    unit_price        FLOAT64,           -- avg_bill_won / avg_usage_kwh
    
    usage_yoy_pct     FLOAT64,           -- 전년 동월 대비 변화율
    usage_mom_pct     FLOAT64            -- 전월 대비 변화율
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2016, 2030, 1))
CLUSTER BY sd_name, sgg_name;
```

Fact data의 스키마입니다.

## 작업 과정
- API의 데이터 수집 기간은 어떻게 확인하였을까?
- Transform 과정은 어떻게 되나요?
- 계절을 나누는 기준은?
