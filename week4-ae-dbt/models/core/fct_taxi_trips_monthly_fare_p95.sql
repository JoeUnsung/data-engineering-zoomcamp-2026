{{ config(materialized='table') }}

with trips as (
    select
        service_type,
        fare_amount,
        trip_distance,
        payment_type_description,
        pickup_datetime,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month
    from {{ ref('fct_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
)

select
    service_type,
    year,
    month,
    percentile_cont(fare_amount, 0.97) over (partition by service_type, year, month) as p97,
    percentile_cont(fare_amount, 0.95) over (partition by service_type, year, month) as p95,
    percentile_cont(fare_amount, 0.90) over (partition by service_type, year, month) as p90
from trips
qualify row_number() over (partition by service_type, year, month order by fare_amount) = 1
