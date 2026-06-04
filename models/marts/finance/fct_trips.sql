-- models/marts/finance/fct_trips.sql
-- ═══════════════════════════════════════════════════════════════
-- PRIMARY FACT TABLE: NYC Yellow Taxi trips
-- Grain: one row per trip
-- Partitioning: pickup_date (for efficient date-range BI scans)
--
-- Senior engineering notes:
--   1. All FKs validated via schema.yml relationships tests
--   2. Revenue decomposition follows TLC official fare structure
--   3. Window functions partitioned to limit shuffle/sort scope
--   4. Computed in dbt so logic is version-controlled + testable
-- ═══════════════════════════════════════════════════════════════

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'trip_sk',
        on_schema_change = 'sync_all_columns',
        partition_by  = {'field': 'pickup_date', 'data_type': 'date'},
        cluster_by    = ['pickup_location_id', 'pickup_hour'],
        tags          = ['gold', 'finance', 'daily'],
        meta          = {
            'owner':       'data-engineering',
            'description': 'Core trip fact table. Source of truth for all revenue analytics.',
            'sla_hours':   2,
        }
    )
}}

with silver as (

    select * from {{ ref('stg_yellow_trips') }}

    -- Incremental: only process new partitions
    {% if is_incremental() %}
    where pickup_date > (select max(pickup_date) from {{ this }})
    {% endif %}

),

date_spine as (

    select * from {{ ref('dim_date') }}

),

locations as (

    select * from {{ ref('dim_location') }}

),

enriched as (

    select
        -- ── Surrogate key ──────────────────────────────────────────────
        s.trip_sk,

        -- ── Foreign keys ───────────────────────────────────────────────
        d.date_id,
        s.vendor_id,
        s.pickup_location_id,
        s.dropoff_location_id,

        -- ── Time grain ─────────────────────────────────────────────────
        s.pickup_ts,
        s.dropoff_ts,
        s.pickup_date,
        s.pickup_hour,
        s.pickup_dow,
        s.time_of_day,
        s.is_weekend,
        s.is_rush_hour,

        -- ── Trip measures ──────────────────────────────────────────────
        s.passenger_count,
        s.trip_distance,
        s.trip_duration_min,
        s.speed_mph,
        s.rate_code,

        -- ── Revenue decomposition (TLC fare structure) ─────────────────
        s.fare_amount,
        s.extra,                      -- Rush/overnight extra
        s.mta_tax,                    -- MTA state surcharge ($0.50)
        s.tip_amount,
        s.tolls_amount,
        s.improvement_surcharge,      -- NYC improvement surcharge ($1.00)
        s.congestion_surcharge,       -- CBD congestion pricing ($2.50)
        s.airport_fee,                -- Airport trip fee
        s.total_amount,
        s.revenue_excl_tips,

        -- ── Derived financial metrics ──────────────────────────────────
        s.tip_rate,
        s.fare_per_mile,
        s.fare_per_minute,

        -- ── Flags ──────────────────────────────────────────────────────
        s.payment_type,
        s.payment_label,
        s.is_airport_trip,
        s.is_jfk_trip,
        s.is_newark_trip,
        s.is_same_zone,
        s.in_congestion_zone,

        -- ── Borough context (from dim_location) ────────────────────────
        pu_loc.borough                as pickup_borough,
        do_loc.borough                as dropoff_borough,
        pu_loc.service_zone           as pickup_service_zone,

        -- ── Window analytics ───────────────────────────────────────────
        -- 7-day rolling average fare for this pickup zone
        round(
            avg(s.fare_amount) over (
                partition by s.pickup_location_id
                order by s.pickup_ts
                rows between 7 * 24 * 6 preceding and current row  -- ~7 days of 10-min buckets
            ), 2
        ) as zone_7d_rolling_avg_fare,

        -- Fare percentile within same zone-hour bucket
        round(
            percent_rank() over (
                partition by s.pickup_location_id, s.pickup_hour
                order by s.fare_amount
            ), 4
        ) as fare_pctile_zone_hour,

        -- Running daily trip count per zone (for demand surge detection)
        count(*) over (
            partition by s.pickup_date, s.pickup_location_id
            order by s.pickup_ts
            rows between unbounded preceding and current row
        ) as zone_daily_running_count,

        -- Revenue rank within pickup date (top earner identification)
        rank() over (
            partition by s.pickup_date
            order by s.total_amount desc
        ) as daily_revenue_rank

    from silver s
    left join date_spine  d      on d.date_day         = s.pickup_date
    left join locations   pu_loc on pu_loc.location_id = s.pickup_location_id
    left join locations   do_loc on do_loc.location_id = s.dropoff_location_id

)

select * from enriched
