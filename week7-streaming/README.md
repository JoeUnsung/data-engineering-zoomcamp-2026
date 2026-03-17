# Week 7: Streaming with Kafka (Redpanda) & PyFlink

## 핵심 컨셉

### 1. Message Broker (Kafka / Redpanda)

Producer → **Broker (Topic)** → Consumer의 pub/sub 구조.

- **Redpanda**: Kafka 프로토콜 100% 호환, C++로 작성되어 JVM/ZooKeeper 불필요
- **Topic**: 메시지가 저장되는 카테고리. Partition으로 병렬 처리 가능
- **Offset**: 각 메시지의 위치. `earliest`(처음부터) / `latest`(최신부터) 선택

### 2. Producer / Consumer 패턴

```
Producer (Python) --serialize--> Kafka Topic --deserialize--> Consumer (Python)
```

- `KafkaProducer`: 데이터를 JSON 직렬화하여 토픽에 전송
- `KafkaConsumer`: 토픽에서 읽어 역직렬화. `group_id`로 소비자 그룹 관리
- 단순 consumer는 집계/윈도우 처리에 한계 → Flink 같은 스트림 프레임워크 필요

### 3. Stream Processing (Apache Flink)

단순 consumer와 달리 **윈도우 집계, 워터마크, 체크포인팅, 병렬처리**를 프레임워크가 관리.

- **Flink SQL**: DDL로 소스(Kafka)/싱크(PostgreSQL) 테이블 정의 → INSERT SQL로 스트리밍 파이프라인 구성
- **JobManager**: 잡 스케줄링/조정 (마스터)
- **TaskManager**: 실제 데이터 처리 (워커)

### 4. Window 종류

| 종류 | 설명 | 예시 |
|------|------|------|
| **Tumbling** | 고정 크기, 겹침 없음 | 매 5분/1시간 단위 집계 |
| **Session** | 비활동 gap 기준 동적 윈도우 | 5분 동안 이벤트 없으면 세션 종료 |
| **Sliding** | 고정 크기, 겹침 있음 | 1시간 윈도우를 15분마다 슬라이드 |

### 5. Watermark & Event Time

- **Event Time**: 데이터가 실제 발생한 시간 (vs Processing Time: 처리 시점)
- **Watermark**: "여기까지 데이터가 다 도착했다"를 알려주는 메커니즘
- `WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND` → 5초 지연 허용
- Watermark가 윈도우 끝을 넘어야 결과 출력됨

### 6. 전체 아키텍처

```
Green Taxi Parquet
        │
        ▼
  Python Producer ──▶ Redpanda (Kafka) ──▶ PyFlink ──▶ PostgreSQL
   (kafka-python)      (Message Broker)    (Stream     (결과 저장)
                                           Processing)
```

---

## 기술 스택 & 라이브러리

| 구분 | 기술 | 용도 |
|------|------|------|
| Message Broker | Redpanda v25.3.9 | Kafka 호환 메시지 브로커 |
| Stream Processing | Apache Flink 2.2.0 | 윈도우 집계, SQL 기반 스트림 처리 |
| Database | PostgreSQL 18 | 처리 결과 저장 |
| Python | kafka-python | Kafka Producer/Consumer |
| Python | pandas, pyarrow | Parquet 데이터 읽기 |
| Python | psycopg2-binary | PostgreSQL 연결 |
| Docker | docker-compose | 인프라 오케스트레이션 |
| Package Manager | uv | Python 의존성 관리 |

---

## 프로젝트 구조

```
week7-streaming/
└── workshop/
    ├── docker-compose.yml          # Redpanda, PostgreSQL, Flink 서비스 정의
    ├── Dockerfile.flink            # PyFlink 커스텀 이미지 (JDBC, Kafka 커넥터 포함)
    ├── flink-config.yaml           # Flink JVM 옵션 및 리소스 설정
    ├── pyproject.toml              # Python 의존성 (kafka-python, pandas 등)
    ├── pyproject.flink.toml        # Flink 컨테이너용 Python 의존성
    ├── uv.lock                     # 의존성 lock 파일
    └── src/
        ├── models.py               # GreenRide 데이터 클래스, 직렬화/역직렬화
        ├── producers/
        │   └── producer.py         # Q2: 데이터 → Redpanda 전송
        ├── consumers/
        │   └── consumer.py         # Q3: trip_distance > 5 카운트
        └── job/
            ├── tumbling_pickup_job.py  # Q4: 5분 tumbling window (PULocationID별 trips)
            ├── session_streak_job.py   # Q5: 5분 gap session window (longest streak)
            └── tumbling_tip_job.py     # Q6: 1시간 tumbling window (total tip)
```

---

## 실행 순서

```bash
# 1. 인프라 시작
cd week7-streaming/workshop
docker compose build
docker compose up -d

# 2. 토픽 생성
docker exec workshop-redpanda-1 rpk topic create green-trips

# 3. PostgreSQL 테이블 생성
docker exec workshop-postgres-1 psql -U postgres -d postgres -c "
CREATE TABLE green_trips_tumbling_pickup (window_start TIMESTAMP, PULocationID INTEGER, num_trips BIGINT, PRIMARY KEY (window_start, PULocationID));
CREATE TABLE green_trips_session_streak (window_start TIMESTAMP, window_end TIMESTAMP, PULocationID INTEGER, num_trips BIGINT, PRIMARY KEY (window_start, PULocationID));
CREATE TABLE green_trips_tumbling_tip (window_start TIMESTAMP, total_tip DOUBLE PRECISION, PRIMARY KEY (window_start));
"

# 4. 데이터 전송
uv run python src/producers/producer.py

# 5. Consumer 실행 (Q3)
uv run python src/consumers/consumer.py

# 6. Flink Job 제출 (하나씩 실행)
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/tumbling_pickup_job.py --pyFiles /opt/src -d
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/session_streak_job.py --pyFiles /opt/src -d
docker exec workshop-jobmanager-1 flink run -py /opt/src/job/tumbling_tip_job.py --pyFiles /opt/src -d

# 7. 정리
docker compose down -v
```

---

## Homework 정답

| 문제 | 답 |
|------|-----|
| **Q1.** Redpanda version | **v25.3.9** |
| **Q2.** Sending data to Redpanda | **60 seconds** |
| **Q3.** Consumer - trip distance > 5 | **8506** |
| **Q4.** Tumbling window - pickup location | **74** |
| **Q5.** Session window - longest streak | **51** |
| **Q6.** Tumbling window - largest tip | **2025-10-16 18:00:00** |
