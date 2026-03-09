# Week 6: Batch Processing with Spark

This week focuses on batch data processing using Apache Spark and PySpark, including DataFrame operations, partitioning, SQL queries, and the Spark UI.

---

## Setup

### Prerequisites
- Python 3.x
- Java (OpenJDK 17)
- PySpark

### Installation
```bash
# Java 설치 (macOS)
brew install openjdk@17

# JAVA_HOME 설정
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# PySpark 설치
cd week6-batch-spark
python3 -m venv venv
source venv/bin/activate
pip install pyspark
```

### Data Download
- [Yellow Taxi Trip Data - November 2025](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

```bash
mkdir -p data
curl -L -o data/yellow_tripdata_2025-11.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
curl -L -o data/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### Run Homework
```bash
python homework.py
```

---

## Homework

### Question 1. Install Spark and PySpark (1 point)

**Answer:** `4.1.1`

```python
spark = SparkSession.builder.master("local[*]").appName("Homework6").getOrCreate()
print(spark.version)  # 4.1.1
```

**Key Learning:**
- `SparkSession`은 Spark 2.0부터 모든 기능의 진입점 역할
- `local[*]`는 로컬 모드에서 모든 가용 코어를 사용

---

### Question 2. Yellow November 2025 (1 point)

**Options:** 6MB / 25MB / 75MB / 100MB

**Answer:** **25MB** (실측 24.4MB)

```python
df = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")
df.repartition(4).write.parquet("data/yellow_nov_2025_4parts", mode="overwrite")
```

```
4개 파일 평균: 24.4 MB
```

**Key Learning:**
- `repartition(n)`은 데이터를 n개 파티션으로 균등 분배 (full shuffle 발생)
- 파티션 수는 병렬 처리 성능과 파일 크기에 직접 영향
- 원본 ~68MB / 4파티션 = ~24MB/파일

---

### Question 3. Count records (1 point)

**Options:** 62,610 / 102,340 / 162,604 / 225,768

**Answer:** **162,604**

```python
from pyspark.sql import functions as F

df.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()
# 162,604
```

**Key Learning:**
- `to_date()`로 timestamp에서 날짜만 추출하여 필터링
- Spark는 predicate pushdown으로 parquet 파일에서 필요한 row group만 읽어 성능 최적화

---

### Question 4. Longest trip (1 point)

**Options:** 22.7 / 58.2 / 90.6 / 134.5

**Answer:** **90.6 hours**

```python
df_with_duration = df.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)
df_with_duration.agg(F.max("duration_hours")).collect()[0][0]
# 90.6
```

**Key Learning:**
- `unix_timestamp()`으로 timestamp를 초 단위 숫자로 변환하여 시간 차이 계산
- `withColumn()`으로 파생 컬럼을 추가하는 것이 Spark DataFrame의 일반적인 패턴
- 90시간이 넘는 trip은 데이터 품질 이슈일 가능성이 높음

---

### Question 5. User Interface (1 point)

**Options:** 80 / 443 / 4040 / 8080

**Answer:** **4040**

**Key Learning:**
- Spark UI는 기본적으로 `http://localhost:4040`에서 실행
- Jobs, Stages, Storage, Environment, Executors 탭 제공
- 여러 SparkSession이 동시에 실행되면 4041, 4042 등 순차적으로 포트 할당
- Spark UI를 통해 실행 계획, 셔플 크기, 메모리 사용량 등을 모니터링

---

### Question 6. Least frequent pickup location zone (1 point)

**Options:**
- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

**Answer:** **Governor's Island/Ellis Island/Liberty Island**

```python
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
pickup_counts = df.groupBy("PULocationID").count()

joined = zones.join(pickup_counts, zones.LocationID == pickup_counts.PULocationID, "left") \
    .fillna(0, subset=["count"])

joined.orderBy("count").select("Zone", "count").show(5)
```

```
+---------------------------------------------+-----+
|Zone                                         |count|
+---------------------------------------------+-----+
|Charleston/Tottenville                       |0    |
|Freshkills Park                              |0    |
|Governor's Island/Ellis Island/Liberty Island|0    |
|Governor's Island/Ellis Island/Liberty Island|0    |
|Great Kills Park                             |0    |
+---------------------------------------------+-----+
```

**Key Learning:**
- `left join`으로 모든 zone을 유지하면서 trip이 없는 zone도 확인 가능
- `fillna(0)`으로 NULL을 0으로 치환하여 정렬 시 최소값으로 표시
- 보기 4개 중 Governor's Island/Ellis Island/Liberty Island이 count=0으로 가장 적음
- 섬 지역은 택시 이용이 거의 없는 것이 자연스러운 결과

---

## Summary

### Technologies Used
- **Apache Spark / PySpark 4.1.1**: 분산 데이터 처리 프레임워크
- **OpenJDK 17**: Spark 실행에 필요한 JVM
- **Parquet**: 컬럼 기반 파일 포맷

### Key Learnings

1. **Spark 아키텍처**: SparkSession → Driver → Executors 구조로 로컬/클러스터 모드 지원
2. **Partitioning**: `repartition()`으로 데이터 분배, 파티션 수가 병렬 처리 성능 결정
3. **DataFrame API**: `filter()`, `withColumn()`, `groupBy()`, `join()` 등 SQL-like 연산
4. **Spark UI (port 4040)**: 실행 중인 Job/Stage/Task를 실시간 모니터링
5. **Data Quality**: 실제 데이터에는 비정상적인 trip duration 등 이상값 존재

### Files in This Directory

- `homework.py`: 전체 과제 풀이 PySpark 스크립트
- `data/`: Yellow Taxi parquet 및 Taxi Zone CSV (gitignored)
- `venv/`: Python 가상환경 (gitignored)

---

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Data Engineering Zoomcamp - Module 6](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-batch)
