# data-engineering-zoomcamp-2026
- Data Engineering Zoom Camp 2026 Cohort

-----


# Homework

## Question 1. What's the version of pip in the python:3.13 image? (1 point)

**Command:**
```bash
docker run python:3.13 pip --version
```

**Output:**
```
Digest: sha256:c8b03b4e98b39cfb180a5ea13ae5ee39039a8f75ccf52fe6d5c216eed6e1be1d
Status: Downloaded newer image for python:3.13
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

**Answer:** 25.3

---

## Question 2. Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database? (1 point)

**Options:**
- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

**Answer:** 
 - postgres:5432


왜 localhost가 아닐까?
 - localhost는 pgadmin 컨테이너 자신을 의미
 - 5432:5432는 호스트 머신에서 접속할 때만 사용
 - 컨테이너 간 통신은 서비스 이름 사용!

---

## Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? (1 point)

**Options:**
- 7,853
- 8,007
- 8,254
- 8,421

**SQL Query:**
```sql
SELECT COUNT(*) as count 
FROM green_taxi_trips 
WHERE trip_distance <= 1.0 
AND EXTRACT(YEAR FROM lpep_pickup_datetime) = 2025 
AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 11;
```

**Used Code:**
```python
# Download Green Taxi data
curl -o green_tripdata_2025-11.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet

# Ingest parquet data to PostgreSQL
python ingest_green_parquet.py
```

**Answer:** 8,007

**Key Learning:**
- Green Taxi 데이터와 Yellow Taxi 데이터는 다르다 (Green은 47k records, Yellow은 4M+ records)
- Green Taxi는 `lpep_pickup_datetime`, Yellow Taxi는 `tpep_pickup_datetime` 사용
- Parquet 파일 처리 방법 학습 

---

## Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles. (1 point)

**Options:**
- 2025-11-14
- 2025-11-20
- 2025-11-23
- 2025-11-25

**SQL Query:**
```sql
SELECT 
    DATE(lpep_pickup_datetime) as pickup_date,
    MAX(trip_distance) as max_trip_distance
FROM green_taxi_trips 
WHERE trip_distance < 100.0
AND EXTRACT(YEAR FROM lpep_pickup_datetime) = 2025 
AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 11
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
```

**Answer:** 2025-11-14 (88.03 miles)

**Key Learning:**
- GROUP BY와 MAX() 함수를 사용한 일별 최대값 찾기
- DATE() 함수로 날짜만 추출하여 그룹핑하는 방법 

---

## Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? (1 point)

**Options:**
- East Harlem North
- East Harlem South
- Morningside Heights
- Forest Hills

**SQL Query:**
```sql
SELECT 
    tz."Zone",
    SUM(gt.total_amount) as total_amount_sum,
    COUNT(*) as trip_count
FROM green_taxi_trips gt
JOIN taxi_zones tz ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
GROUP BY tz."Zone", tz."LocationID"
ORDER BY total_amount_sum DESC;
```

**Used Code:**
```python
# Download zone lookup table
curl -o taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# Load to PostgreSQL
zones_df = pd.read_csv('taxi_zone_lookup.csv')
zones_df.to_sql('taxi_zones', engine, if_exists='replace', index=False)
```

**Answer:** East Harlem North ($9,281.92)

**Key Learning:**
- JOIN을 사용한 테이블 연결 (Location ID를 Zone 이름으로 변환)
- SUM() 집계 함수 사용법
- Taxi Zone Lookup 테이블의 중요성 

---

## Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point)

**Options:**
- JFK Airport
- Yorkville West
- East Harlem North
- LaGuardia Airport

**SQL Query:**
```sql
SELECT 
    do_tz."Zone" as dropoff_zone,
    MAX(gt.tip_amount) as max_tip,
    COUNT(*) as trip_count
FROM green_taxi_trips gt
JOIN taxi_zones pu_tz ON gt."PULocationID" = pu_tz."LocationID"
JOIN taxi_zones do_tz ON gt."DOLocationID" = do_tz."LocationID"
WHERE pu_tz."Zone" = 'East Harlem North'
AND EXTRACT(YEAR FROM gt.lpep_pickup_datetime) = 2025 
AND EXTRACT(MONTH FROM gt.lpep_pickup_datetime) = 11
GROUP BY do_tz."Zone", do_tz."LocationID"
ORDER BY max_tip DESC;
```

**Answer:** Yorkville West ($81.89 max tip)

**Key Learning:**
- 복수의 JOIN 사용 (pickup zone과 dropoff zone 모두)
- MAX() 함수로 최대 팁 금액 찾기
- 특정 조건 하에서의 그룹별 최대값 분석 

---

## Question 7. Which of the following sequences describes the Terraform workflow for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources? (1 point)

**Options:**
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

**Answer:** terraform init, terraform apply -auto-approve, terraform destroy

**Key Learning:**
- **terraform init**: 플러그인 다운로드 및 백엔드 설정 (워크플로우의 첫 단계)
- **terraform apply**: 변경사항 생성 및 실행 (`-auto-approve` 플래그로 자동 승인)
- **terraform destroy**: 모든 리소스 제거
- `terraform import`는 기존 리소스를 가져올 때 사용 (워크플로우 시작이 아님)
- `terraform plan`은 미리보기만 제공 (실제 실행 안함)
- `terraform run`, `terraform rm`은 존재하지 않는 명령어

---

## Summary

**Technologies Used:**
- Docker & Docker Compose
- PostgreSQL
- Python with pandas, SQLAlchemy
- Parquet file format
- Terraform (concept)

**Key Learnings:**
1. **Data Sources**: Green Taxi vs Yellow Taxi 데이터의 차이점과 적절한 데이터 선택의 중요성
2. **Container Networking**: 컨테이너 간 통신에서 service name 사용법
3. **Data Ingestion**: Parquet 파일을 PostgreSQL로 효율적으로 적재하는 방법
4. **SQL Analysis**: JOIN, GROUP BY, 집계 함수를 활용한 복합 데이터 분석
5. **Infrastructure**: Terraform의 기본 워크플로우 이해