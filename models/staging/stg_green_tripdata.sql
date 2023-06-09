{{ config(materialized="view") }}

select 
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime'])}} as trip_id,
    cast(vendorid as numeric) as vendorid,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    cast(PULocationID as int) as pu_loc_id,
    cast(DOLocationID as int) as do_loc_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
{{ get_payment_type_description('payment_type') }} as pay_type_desc
from {{ source('staging', 'grn_dbt') }}
where vendorid is not null

{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
