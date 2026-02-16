{{ config(materialized='table') }}

with trips as (
    select
        pickup_locationid as revenue_zone,
        service_type,
        pickup_datetime,
        total_amount,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month
    from {{ ref('fct_trips') }}
)

select
    revenue_zone,
    service_type,
    year,
    month,
    count(*) as total_trips,
    sum(total_amount) as total_revenue
from trips
group by revenue_zone, service_type, year, month
