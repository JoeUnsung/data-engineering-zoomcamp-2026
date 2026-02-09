-- External Table 생성
CREATE OR REPLACE EXTERNAL TABLE `taxi_data.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-*.parquet']
);

-- Materialized Table 생성 (External Table에서 데이터 복사)
CREATE OR REPLACE TABLE `taxi_data.yellow_taxi_materialized` AS
SELECT * FROM `taxi_data.yellow_taxi_external`;

-- ========================================
-- Question 1: 2024 Yellow Taxi 데이터의 레코드 수
-- ========================================
SELECT COUNT(*) AS record_count
FROM `taxi_data.yellow_taxi_materialized`;

-- ========================================
-- Question 2: External Table vs Materialized Table 예상 데이터 읽기량
-- ========================================
-- External Table에서 PULocationID 조회 (예상 바이트 확인)
SELECT COUNT(DISTINCT PULocationID)
FROM `taxi_data.yellow_taxi_external`;

-- Materialized Table에서 PULocationID 조회 (예상 바이트 확인)
SELECT COUNT(DISTINCT PULocationID)
FROM `taxi_data.yellow_taxi_materialized`;

-- ========================================
-- Question 3: 1개 컬럼 vs 2개 컬럼 조회 시 예상 바이트 차이
-- ========================================
-- 1개 컬럼 조회
SELECT COUNT(DISTINCT PULocationID)
FROM `taxi_data.yellow_taxi_materialized`;

-- 2개 컬럼 조회
SELECT COUNT(DISTINCT PULocationID), COUNT(DISTINCT DOLocationID)
FROM `taxi_data.yellow_taxi_materialized`;

-- ========================================
-- Question 4: fare_amount가 0인 레코드 수
-- ========================================
SELECT COUNT(*) AS zero_fare_count
FROM `taxi_data.yellow_taxi_materialized`
WHERE fare_amount = 0;

-- ========================================
-- Question 5: 최적화된 테이블 생성 (Partition + Cluster)
-- ========================================
-- tpep_dropoff_datetime으로 Partition, VendorID로 Cluster
CREATE OR REPLACE TABLE `taxi_data.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `taxi_data.yellow_taxi_materialized`;

-- ========================================
-- Question 6: Partitioned vs Non-Partitioned 테이블 비교
-- ========================================
-- Non-partitioned (Materialized) 테이블에서 조회
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_materialized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned 테이블에서 조회
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- ========================================
-- Question 9: Materialized Table에서 COUNT(*) 예상 바이트
-- ========================================
SELECT COUNT(*)
FROM `taxi_data.yellow_taxi_materialized`;