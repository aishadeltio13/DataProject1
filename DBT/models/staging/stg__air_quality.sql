with source as (
    select * from {{ source('london_source', 'registroaire') }}
),

renamed as (
    select
        id as record_id,
        source as data_origin,
        station_uid,
        station_name,
        lat,
        lon,
        cast(sensor_date as timestamp) as sensor_date,
        cast(scraped_at as timestamp)  as register_date,
        parameter,
        case when value >= 0 then value else 0 end as measurement_value,    -- only accept positive values
        unit
    from source
)

select * from renamed



