-- models/intermediate/int_trips_enriched.sql
-- ════════════════════════════════════════════════════════════════
-- Intermediate: join staging trips with dim_location to add borough
-- context. Materialised as a table for performance (used by 2+ marts).
-- ════════════════════════════════════════════════════════════════

{{ config(
    materialized='table',
    tags=['intermediate'],
) }}

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

locations as (

    select * from {{ ref('dim_location') }}

)

select
    t.*,

    -- Pickup borough context
    pu.borough                  as pickup_borough,
    pu.service_zone             as pickup_service_zone,
    pu.is_airport               as pickup_is_airport_zone,
    pu.is_manhattan             as pickup_is_manhattan,

    -- Dropoff borough context
    do.borough                  as dropoff_borough,
    do.service_zone             as dropoff_service_zone,

    -- Cross-borough trip flag
    (pu.borough != do.borough)  as is_cross_borough_trip

from trips t
left join locations pu on pu.location_id = t.pickup_location_id
left join locations do_ on do_.location_id = t.dropoff_location_id
