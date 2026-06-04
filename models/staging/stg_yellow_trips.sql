-- models/staging/stg_yellow_trips.sql
-- ════════════════════════════════════════════════════════════════
-- Staging layer: rename columns to business vocabulary,
-- cast types, add surrogate key.
--
-- This is the ONLY place raw source column names appear.
-- All downstream models reference stg_yellow_trips, not the source.
-- ════════════════════════════════════════════════════════════════

{{ config(
    materialized='view',
    tags=['staging'],
    meta={'owner': 'data-engineering', 'source': 'nyc_tlc.clean_trips'}
) }}

with source as (

    select * from {{ source('nyc_tlc', 'clean_trips') }}

),

renamed as (

    select
        -- ── Surrogate key (5-column hash; resolves same-second collisions) ──
        hash(
            vendorid::varchar || '|' ||
            tpep_pickup_datetime::varchar || '|' ||
            tpep_dropoff_datetime::varchar || '|' ||
            pulocationid::varchar || '|' ||
            fare_amount::varchar
        ) % 9223372036854775807                     as trip_sk,

        -- ── Identifiers ───────────────────────────────────────────────────
        vendorid                                    as vendor_id,
        pulocationid                                as pickup_location_id,
        dolocationid                                as dropoff_location_id,
        ratecodeid                                  as rate_code,
        payment_type,

        -- ── Timestamps ────────────────────────────────────────────────────
        tpep_pickup_datetime                        as pickup_ts,
        tpep_dropoff_datetime                       as dropoff_ts,
        cast(tpep_pickup_datetime as date)          as pickup_date,

        -- ── Pre-derived time features (from silver) ───────────────────────
        derived_pickup_hour                         as pickup_hour,
        derived_pickup_dow                          as pickup_dow,
        derived_is_weekend                          as is_weekend,
        derived_is_rush_hour                        as is_rush_hour,
        derived_time_of_day                         as time_of_day,

        -- ── Trip dimensions ───────────────────────────────────────────────
        coalesce(passenger_count, 1)                as passenger_count,
        trip_distance,
        derived_trip_duration_min                   as trip_duration_min,
        derived_speed_mph                           as speed_mph,

        -- ── Fare components ───────────────────────────────────────────────
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        coalesce(congestion_surcharge, 0)           as congestion_surcharge,
        coalesce(airport_fee, 0)                    as airport_fee,
        total_amount,
        derived_revenue_excl_tips                   as revenue_excl_tips,

        -- ── Derived financial ratios (from silver) ────────────────────────
        derived_tip_rate                            as tip_rate,
        derived_fare_per_mile                       as fare_per_mile,
        derived_fare_per_minute                     as fare_per_minute,

        -- ── Derived flags (from silver) ───────────────────────────────────
        derived_is_airport_trip                     as is_airport_trip,
        derived_is_jfk_trip                         as is_jfk_trip,
        derived_is_newark_trip                      as is_newark_trip,
        derived_is_same_zone                        as is_same_zone,
        derived_in_congestion_zone                  as in_congestion_zone,
        derived_payment_label                       as payment_label,
        store_and_fwd_flag

    from source

)

select * from renamed
