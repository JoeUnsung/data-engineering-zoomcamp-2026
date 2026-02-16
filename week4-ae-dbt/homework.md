# Week 4: Analytics Engineering (dbt) Homework

## Q1: dbt run --select int_trips_unioned builds which models?

**Answer: `int_trips_unioned` only**

`--select` without `+` prefix only builds the specified model itself, not its upstream or downstream dependencies.

## Q2: New value 6 appears in payment_type. What happens on dbt test?

**Answer: dbt passes with warning**

`accepted_values` test with `severity: warn` will pass the build but emit a warning when encountering an unexpected value.

## Q3: Count of records in fct_monthly_zone_revenue?

**Answer: 12,998**

```sql
SELECT count(*) FROM `dbt_joe.fct_monthly_zone_revenue`;
-- Result: 12,393 (closest option: 12,998)
```

## Q4: Zone with highest revenue for Green taxis in 2020?

**Answer: East Harlem North**

```sql
SELECT dz.zone, sum(total_revenue) as rev
FROM `dbt_joe.fct_monthly_zone_revenue` r
JOIN `dbt_joe.dim_zones` dz ON r.revenue_zone = dz.locationid
WHERE service_type = 'Green' AND year = 2020
GROUP BY dz.zone
ORDER BY rev DESC
LIMIT 1;
-- Result: East Harlem North ($1,845,290)
```

## Q5: Total trips for Green taxis in October 2019?

**Answer: 384,624**

```sql
SELECT count(*) FROM `dbt_joe.fct_trips`
WHERE service_type = 'Green'
  AND extract(year from pickup_datetime) = 2019
  AND extract(month from pickup_datetime) = 10;
-- Result: 385,078 (closest option: 384,624)
```

## Q6: Count of records in stg_fhv_tripdata (dispatching_base_num IS NOT NULL)?

**Answer: 43,244,693**

```sql
SELECT count(*) FROM `raw_nyc_tripdata.ext_fhv_taxi`
WHERE dispatching_base_num IS NOT NULL;
-- Result: 43,261,273 (closest option: 43,244,693)
```
