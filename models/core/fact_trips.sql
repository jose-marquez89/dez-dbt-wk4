{{ config(materialized='table') }}

with green as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),

trips_unioned as (
    select * from green
    union all
    select * from yellow
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough <> 'Unknown'
)

select *
from trips_unioned t
join dim_zones pz
    on t.pu_loc_id = pz.locationid
join dim_zones dz
    on t.do_loc_id = dz.locationid
