with all_calculated_data as (
    -- Reed from model with AQI calculated
    select * from {{ ref('int__london__aqi_calculations') }}
),

historical_only as (
    -- Keep only the ones from historical data
    select * from all_calculated_data
    where data_origin = 'historical_data'
),

