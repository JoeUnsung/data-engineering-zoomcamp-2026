# Week 7: Streaming with Kafka (Redpanda) & PyFlink

## Homework

Workshop: [PyFlink Stream Processing Workshop](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming/workshop)

Data: [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)

### Question 1. Redpanda version

```bash
docker exec workshop-redpanda-1 rpk version
```

**Answer: v25.3.9**

### Question 2. Sending data to Redpanda

Green taxi 데이터(49,416건)를 `green-trips` 토픽에 전송하는 시간 측정.

**Answer: 60 seconds**

### Question 3. Consumer - trip distance

`trip_distance > 5.0`인 trip 수를 카운트.

```
Total messages: 49416
Trips with distance > 5.0: 8506
```

**Answer: 8506**

### Question 4. Tumbling window - pickup location

5분 tumbling window로 PULocationID별 trip count 집계.

```sql
SELECT PULocationID, num_trips
FROM green_trips_tumbling_pickup
ORDER BY num_trips DESC
LIMIT 3;
```

| PULocationID | num_trips |
|---|---|
| 74 | 15 |
| 74 | 14 |
| 74 | 13 |

**Answer: 74**

### Question 5. Session window - longest streak

5분 gap의 session window로 PULocationID별 longest streak 확인.

**Answer: TBD**

### Question 6. Tumbling window - largest tip

1시간 tumbling window로 시간대별 total tip_amount 집계.

**Answer: TBD**
