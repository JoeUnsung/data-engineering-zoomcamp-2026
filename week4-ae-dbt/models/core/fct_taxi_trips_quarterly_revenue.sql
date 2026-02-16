{{ config(materialized='table') }}

with trips as (
    select
        service_type,
        cast(total_amount as numeric) as total_amount,
        pickup_datetime,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        extract(month from pickup_datetime) as month
    from {{ ref('fct_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
),

quarterly_revenue as (
    select
        service_type,
        year,
        quarter,
        concat(cast(year as string), '/Q', cast(quarter as string)) as year_quarter,
        sum(total_amount) as quarterly_revenue
    from trips
    group by service_type, year, quarter
),

yoy_growth as (
    select
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.year_quarter,
        curr.quarterly_revenue,
        prev.quarterly_revenue as prev_year_revenue,
        case
            when prev.quarterly_revenue > 0
            then round((curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue * 100, 2)
            else null
        end as yoy_growth_pct
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.year = prev.year + 1
        and curr.quarter = prev.quarter
)

select * from yoy_growth
order by service_type, year, quarter
