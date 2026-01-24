with all_calculated_data as (
    -- Read from model with AQI calculated
    select * from {{ ref('int__aqi_calculations') }}
),

historical_only as (
    -- Keep only the ones from historical data
    select * from all_calculated_data
    where data_origin = 'historical_data'
),

area_calculations as (
    select
        area_code,     -- Use the cuadrants calculated in int__aqi_calculations
        parameter,     -- Separate each pollutant
        extract(month from sensor_date) as sensor_month, --- Separate months 
        -- Statistical analysis
        avg(aqi_value)      as avg_aqi_zone,
        max(aqi_value)      as max_aqi_zone,
        min(aqi_value)      as min_aqi_zone,
        variance(aqi_value) as variance_aqi_zone,
        stddev(aqi_value)   as stddev_aqi_zone,
        count(*)            as total_samples
    from historical_only
    group by 1, 2, 3 
)

select * from area_calculations