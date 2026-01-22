with

source as (
    select * from {{ source('london_source', 'registroaire') }}
),

renamed as(
    select
        id,
        source,
        station_uid,
        station_name,
        lat,
        lon,
        cast(sensor_date as timestamp)  as sensor_date,
        cast(scraped_at as timestamp)   as register_date,
        parameter,
        greatest(0, value)          as value,
        unit,
        -- Calculation of the AQI value = ((I_high - I_low) / (C_high - C_low)) * (C - C_low) + I_low
        -- I: AQI index value [high and low are the limit values for each category (Good, Moderate, Unhealthy...)]
        -- C: contaminant value (µg/m3)
        case
            when unit = 'aqi' then value    -- if we already have an AQI value, we keep it, do not transform it.
            when parameter = 'pm25' then
                case
                    when value <= 12.0  -- Good:            I {0, 50}       C {0.0, 12.0}
                        then ((50 - 0) / (12.0 - 0)) * (value - 0.0) + 0
                    when value <= 35.4  -- Moderate:        I {51, 100}     C {12.1, 35.4}
                        then ((100 - 51) / (35.4 - 12.1)) * (value - 12.1) + 51
                    when value <= 55.4  -- Sensitive Groups:I {101, 150}    C {35.5, 55.4}
                        then ((150 - 101) / (55.4 - 35.5)) * (value - 35.5) + 101
                    when value <= 150.4 -- Unhealthy:       I {151, 200}    C {55.5, 150.4}
                        then ((200 - 151) / (150.4 - 55.5)) * (value - 55.5) + 151
                    when value <= 250.4 -- Very unhealthy:  I {201, 300}    C {150.5, 250.4}
                        then ((300 - 201) / (250.4 - 150.5)) * (value - 150.5) + 201
                    else                -- Hazardous:       I {301, 500}    C {250.5, 500.4}
                        then ((500 - 301) / (500.4 - 250.5)) * (value - 250.5) + 301
                end
            when parameter = 'pm10' then
                case
                    when value <= 54.0   then ((50.0 - 0.0) / (54.0 - 0.0)) * (value - 0.0) + 0.0
                    when value <= 154.0  then ((100.0 - 51.0) / (154.0 - 55.0)) * (value - 55.0) + 51.0
                    when value <= 254.0  then ((150.0 - 101.0) / (254.0 - 155.0)) * (value - 155.0) + 101.0
                    when value <= 354.0  then ((200.0 - 151.0) / (354.0 - 255.0)) * (value - 255.0) + 151.0
                    when value <= 424.0  then ((300.0 - 201.0) / (424.0 - 355.0)) * (value - 355.0) + 201.0
                    else ((500.0 - 301.0) / (604.0 - 425.0)) * (value - 425.0) + 301.0
                end
            when parameter = 'no2' then
                case
                    when value <= 100.0  then ((50.0 - 0.0) / (100.0 - 0.0)) * (value - 0.0) + 0.0
                    when value <= 188.0  then ((100.0 - 51.0) / (188.0 - 101.0)) * (value - 101.0) + 51.0
                    when value <= 677.0  then ((150.0 - 101.0) / (677.0 - 189.0)) * (value - 189.0) + 101.0
                    when value <= 1221.0 then ((200.0 - 151.0) / (1221.0 - 678.0)) * (value - 678.0) + 151.0
                    when value <= 2348.0 then ((300.0 - 201.0) / (2348.0 - 1222.0)) * (value - 1222.0) + 201.0
                    else ((500.0 - 301.0) / (3853.0 - 2349.0)) * (value - 2349.0) + 301.0
                end
            when parameter = 'o3' then
                case
                    when value <= 106.0  then ((50.0 - 0.0) / (106.0 - 0.0)) * (value - 0.0) + 0.0
                    when value <= 137.0  then ((100.0 - 51.0) / (137.0 - 107.0)) * (value - 107.0) + 51.0
                    when value <= 167.0  then ((150.0 - 101.0) / (167.0 - 138.0)) * (value - 138.0) + 101.0
                    when value <= 206.0  then ((200.0 - 151.0) / (206.0 - 168.0)) * (value - 168.0) + 151.0
                    when value <= 400.0  then ((300.0 - 201.0) / (400.0 - 207.0)) * (value - 207.0) + 201.0
                    else ((500.0 - 301.0) / (600.0 - 401.0)) * (value - 401.0) + 301.0
                end
            else null
        end as aqi_value
        

    from source
)

select * from renamed