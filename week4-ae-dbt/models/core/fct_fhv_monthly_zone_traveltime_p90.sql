{{ config(materialized='table') }}

with fhv_trips as (
    select
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        pickup_datetime,
        dropoff_datetime,
        year,
        month,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_seconds
    from {{ ref('fct_fhv_trips') }}
    where pickup_datetime is not null
      and dropoff_datetime is not null
)

select
    year,
    month,
    pickup_locationid,
    pickup_zone,
    dropoff_locationid,
    dropoff_zone,
    percentile_cont(trip_duration_seconds, 0.90) over (
        partition by year, month, pickup_locationid, dropoff_locationid
    ) as p90_trip_duration
from fhv_trips
qualify row_number() over (
    partition by year, month, pickup_locationid, dropoff_locationid
    order by trip_duration_seconds
) = 1
