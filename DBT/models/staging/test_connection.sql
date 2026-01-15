{{ config(materialized='table') }}

WITH raw_data AS (
    -- Read
    SELECT * FROM {{ source('london_source', 'london_raw_data') }}
)

SELECT
    id,
    ingestion_time,
    -- Just trying :) 
    raw_data->>'station_name' as station,
    raw_data->>'parameter' as pollutant,
    (raw_data->>'value')::numeric as value
FROM raw_data