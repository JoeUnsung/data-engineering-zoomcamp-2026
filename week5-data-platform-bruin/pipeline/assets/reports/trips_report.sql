/* @bruin

name: reports.trips_report
type: duckdb.sql

materialization:
  type: table
  strategy: replace

depends:
  - staging.trips

@bruin */

SELECT
    taxi_type,
    DATE_TRUNC('month', pickup_datetime) AS month,
    COUNT(*)                             AS total_trips,
    AVG(trip_distance)                   AS avg_distance,
    AVG(fare_amount)                     AS avg_fare,
    SUM(total_amount)                    AS total_revenue
FROM staging.trips
GROUP BY 1, 2
ORDER BY 1, 2
