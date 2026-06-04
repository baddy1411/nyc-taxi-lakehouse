-- models/marts/geo/dim_location.sql
-- NYC TLC Taxi Zones (263 zones across 5 boroughs + EWR)

{{ config(materialized='table', tags=['geo', 'dimension']) }}

with zones as (

    select
        location_id,
        borough,
        service_zone,
        is_airport,
        is_manhattan,

        -- Human-readable service zone label
        case service_zone
            when 'Yellow Zone' then 'Manhattan Core'
            when 'Boro Zone'   then 'Outer Borough'
            when 'Airports'    then 'Airport'
            else service_zone
        end as service_zone_label,

        -- Borough group for aggregations
        case
            when borough = 'Manhattan'     then 1
            when borough = 'Brooklyn'      then 2
            when borough = 'Queens'        then 3
            when borough = 'Bronx'         then 4
            when borough = 'Staten Island' then 5
            when borough = 'EWR'           then 6
            else 99
        end as borough_sort_order

    from {{ ref('dim_location_seed') }}

)

select * from zones
order by location_id
