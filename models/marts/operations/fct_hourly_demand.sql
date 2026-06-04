-- models/marts/operations/fct_hourly_demand.sql
-- ════════════════════════════════════════════════════════════════
-- Pre-aggregated demand by (date, hour, zone).
-- This is the fast-path for the 95% BI query pattern:
--   "How many trips picked up from zone X at hour Y on date Z?"
--
-- Computing this once avoids repeated 500K-row scans in BI tools.
-- The demand_wow_ratio column enables week-over-week anomaly detection.
-- ════════════════════════════════════════════════════════════════

{{ config(
    materialized='incremental',
    unique_key=['pickup_date', 'pickup_hour', 'pickup_location_id'],
    tags=['operations', 'daily'],
) }}

with base as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    where pickup_date > (select max(pickup_date) from {{ this }})
    {% endif %}

)

select
    pickup_date,
    pickup_hour,
    pickup_location_id,
    pickup_borough,
    is_weekend,

    -- Volume
    count(*)                                                as trip_count,
    sum(total_amount)                                       as total_revenue,
    sum(revenue_excl_tips)                                  as revenue_excl_tips,

    -- Averages
    round(avg(fare_amount),      2)                         as avg_fare,
    round(avg(tip_rate),         4)                         as avg_tip_rate,
    round(avg(trip_distance),    2)                         as avg_distance,
    round(avg(trip_duration_min),1)                         as avg_duration_min,
    round(avg(speed_mph),        1)                         as avg_speed_mph,

    -- Payment breakdown
    sum(case when payment_type = 1 then 1 else 0 end)       as credit_card_trips,
    sum(case when payment_type = 2 then 1 else 0 end)       as cash_trips,

    -- Special trip types
    sum(case when is_airport_trip   then 1 else 0 end)      as airport_trips,
    sum(case when is_rush_hour      then 1 else 0 end)      as rush_hour_trips,
    sum(case when in_congestion_zone then 1 else 0 end)     as congestion_zone_trips,

    -- Demand trend: this hour vs same hour last week
    round(
        count(*) * 1.0 / nullif(
            lag(count(*), 7) over (
                partition by pickup_hour, pickup_location_id
                order by pickup_date
            ), 0
        ), 4
    )                                                       as demand_wow_ratio

from base
group by
    pickup_date, pickup_hour, pickup_location_id, pickup_borough, is_weekend
order by
    pickup_date, pickup_hour
