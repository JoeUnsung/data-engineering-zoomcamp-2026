{{ config(materialized='view') }}

with tripdata as (
    select *,
        row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('raw_nyc_tripdata', 'ext_fhv_taxi') }}
    where dispatching_base_num is not null
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as integer) as sr_flag,
    Affiliated_base_number as affiliated_base_number

from tripdata
where rn = 1

{% if var('is_test_run', default=false) %}
    limit 100
{% endif %}
