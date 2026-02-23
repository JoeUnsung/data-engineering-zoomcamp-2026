/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  time_granularity: month
  incremental_key: pickup_datetime

depends:
  - ingestion.trips

columns:
  - name: trip_id
    type: VARCHAR
    description: "Unique trip identifier"
    primary_key: true
  - name: taxi_type
    type: VARCHAR
    description: "Type of taxi (yellow or green)"
  - name: pickup_datetime
    type: TIMESTAMP
    description: "Trip pickup timestamp"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: TIMESTAMP
    description: "Trip dropoff timestamp"
    checks:
      - name: not_null
  - name: passenger_count
    type: INTEGER
    description: "Number of passengers"
  - name: trip_distance
    type: FLOAT
    description: "Trip distance in miles"
  - name: fare_amount
    type: FLOAT
    description: "Base fare amount"
    checks:
      - name: positive
  - name: total_amount
    type: FLOAT
    description: "Total amount charged"

@bruin */

SELECT
    gen_random_uuid()::VARCHAR   AS trip_id,
    'yellow'                     AS taxi_type,
    tpep_pickup_datetime         AS pickup_datetime,
    tpep_dropoff_datetime        AS dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
FROM ingestion.yellow_trips
WHERE tpep_pickup_datetime BETWEEN '{{ start_date }}' AND '{{ end_date }}'

UNION ALL

SELECT
    gen_random_uuid()::VARCHAR   AS trip_id,
    'green'                      AS taxi_type,
    lpep_pickup_datetime         AS pickup_datetime,
    lpep_dropoff_datetime        AS dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
FROM ingestion.green_trips
WHERE lpep_pickup_datetime BETWEEN '{{ start_date }}' AND '{{ end_date }}'
