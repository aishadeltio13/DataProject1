with staging as (
    select * from {{ ref('stg__air_quality') }}
),

-- STEP 1: AQI
-- Calculation of the AQI value = ((I_high - I_low) / (C_high - C_low)) * (C - C_low) + I_low
-- I: AQI index value [high and low are the limit values for each aqi_category (Good, Moderate, Unhealthy...)]
-- C: contaminant value (µg/m3)
calc_aqi as (
    select
        *,
        case
            when unit = 'aqi' then measurement_value -- if we already have an AQI value, we keep it, do not transform it.
            when parameter = 'pm25' then
                case
                    when measurement_value <= 12.0  then ((50 - 0) / (12.0 - 0)) * (measurement_value - 0.0) + 0
                    when measurement_value <= 35.4  then ((100 - 51) / (35.4 - 12.1)) * (measurement_value - 12.1) + 51
                    when measurement_value <= 55.4  then ((150 - 101) / (55.4 - 35.5)) * (measurement_value - 35.5) + 101
                    when measurement_value <= 150.4 then ((200 - 151) / (150.4 - 55.5)) * (measurement_value - 55.5) + 151
                    when measurement_value <= 250.4 then ((300 - 201) / (250.4 - 150.5)) * (measurement_value - 150.5) + 201
                    else ((500 - 301) / (500.4 - 250.5)) * (measurement_value - 250.5) + 301
                end
            when parameter = 'pm10' then
                case
                    when measurement_value <= 54.0   then ((50.0 - 0.0) / (54.0 - 0.0)) * (measurement_value - 0.0) + 0.0
                    when measurement_value <= 154.0  then ((100.0 - 51.0) / (154.0 - 55.0)) * (measurement_value - 55.0) + 51.0
                    when measurement_value <= 254.0  then ((150.0 - 101.0) / (254.0 - 155.0)) * (measurement_value - 155.0) + 101.0
                    when measurement_value <= 354.0  then ((200.0 - 151.0) / (354.0 - 255.0)) * (measurement_value - 255.0) + 151.0
                    when measurement_value <= 424.0  then ((300.0 - 201.0) / (424.0 - 355.0)) * (measurement_value - 355.0) + 201.0
                    else ((500.0 - 301.0) / (604.0 - 425.0)) * (measurement_value - 425.0) + 301.0
                end
            when parameter = 'no2' then
                case
                    when measurement_value <= 100.0  then ((50.0 - 0.0) / (100.0 - 0.0)) * (measurement_value - 0.0) + 0.0
                    when measurement_value <= 188.0  then ((100.0 - 51.0) / (188.0 - 101.0)) * (measurement_value - 101.0) + 51.0
                    when measurement_value <= 677.0  then ((150.0 - 101.0) / (677.0 - 189.0)) * (measurement_value - 189.0) + 101.0
                    when measurement_value <= 1221.0 then ((200.0 - 151.0) / (1221.0 - 678.0)) * (measurement_value - 678.0) + 151.0
                    when measurement_value <= 2348.0 then ((300.0 - 201.0) / (2348.0 - 1222.0)) * (measurement_value - 1222.0) + 201.0
                    else ((500.0 - 301.0) / (3853.0 - 2349.0)) * (measurement_value - 2349.0) + 301.0
                end
            when parameter = 'o3' then
                case
                    when measurement_value <= 106.0  then ((50.0 - 0.0) / (106.0 - 0.0)) * (measurement_value - 0.0) + 0.0
                    when measurement_value <= 137.0  then ((100.0 - 51.0) / (137.0 - 107.0)) * (measurement_value - 107.0) + 51.0
                    when measurement_value <= 167.0  then ((150.0 - 101.0) / (167.0 - 138.0)) * (measurement_value - 138.0) + 101.0
                    when measurement_value <= 206.0  then ((200.0 - 151.0) / (206.0 - 168.0)) * (measurement_value - 168.0) + 151.0
                    when measurement_value <= 400.0  then ((300.0 - 201.0) / (400.0 - 207.0)) * (measurement_value - 207.0) + 201.0
                    else ((500.0 - 301.0) / (600.0 - 401.0)) * (measurement_value - 401.0) + 301.0
                end
        end as aqi_value
    from staging
),

-- STEP 2: Category
categorized as (
    select
        *,
        case
            when aqi_value <= 50 then 'Good'
            when aqi_value <= 100 then 'Moderate'
            when aqi_value <= 150 then 'Sensitive Groups'
            when aqi_value <= 200 then 'Unhealthy'
            when aqi_value <= 300 then 'Very unhealthy'
            else 'Hazardous'
        end as aqi_category,
        width_bucket(lat, 51.28, 51.69, 4) as lat_bucket,
        width_bucket(lon, -0.51, 0.33, 8) as lon_bucket
    from calc_aqi
),

-- STEP 3: Message
final as (
    select
        *,
        case
            when aqi_category = 'Good'             then 'Air quality is satisfactory, and air pollution poses little or no risk.'
            when aqi_category = 'Moderate'         then 'Air quality is acceptable. However, there may be a risk for some people, particularly those who are unusually sensitive to air pollution.'
            when aqi_category = 'Sensitive Groups' then 'Members of sensitive groups may experience health effects. The general public is less likely to be affected.'
            when aqi_category = 'Unhealthy'        then 'Some members of the general public may experience health effects; members of sensitive groups may experience more serious health effects.'
            when aqi_category = 'Very unhealthy'   then 'Health alert: The risk of health effects is increased for everyone.'
            else 'Health warning of emergency conditions: everyone is more likely to be affected.'
        end as aqi_message,
        'Q_' || lat_bucket || '_' || lon_bucket as area_code
    from categorized
)

select * from final

