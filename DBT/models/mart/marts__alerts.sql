with realtime_data as (
    select * from {{ ref('int__aqi_calculations') }}
    where data_origin = 'realtime'
),

historical_stats as (
    select * from {{ ref('int__historical_ref') }}
),

joined as (
    select
        rt.record_id,
        rt.station_name,
        rt.area_code,
        rt.sensor_date,
        rt.parameter,
        rt.measurement_value,
        rt.unit,
        rt.aqi_value as realtime_aqi_value,
        rt.aqi_category,
        rt.aqi_message,
        hs.avg_aqi_zone,
        hs.stddev_aqi_zone,
        (rt.aqi_value - hs.avg_aqi_zone) as aqi_diff,
        case
            when hs.avg_aqi_zone > 0 then (rt.aqi_value / hs.avg_aqi_zone) else 1
        end as increase_ratio
        from realtime_data rt
        left join historical_stats hs
            on rt.area_code = hs.area_code
            and rt.parameter = hs.parameter
            and extract(month from rt.sensor_date) = hs.sensor_month
),

final_alerts as (
    select
        *,
        -- 1. ABSOLUT ALERT (based on absolut levels from WHO)
        case            
            when parameter = 'pm25' and measurement_value > 25.0 then True
            when parameter = 'pm10' and measurement_value > 50.0 then True
            when parameter = 'no2' and measurement_value > 200.0 then True
            when parameter = 'o3' and measurement_value > 120.0 then True
            else False
        end as absolute_alert,
        -- 2. SENSITIVE ALERT (based on absolut levels from WHO)
        case
            when parameter = 'pm25' and measurement_value > 15.0 then True
            when parameter = 'pm10' and measurement_value > 45.0 then True
            when parameter = 'no2' and measurement_value > 200.0 then True
            when parameter = 'o3' and measurement_value > 100.0 then True
            else False
        end as sensitive_alert,
        -- 3. RELATIVE ALERT (when pollution is 2.5x higher than normal)
        case when realtime_aqi_value > (avg_aqi_zone + (2.5 * stddev_aqi_zone))
            then True else False
        end as realtive_alert
    
    from joined
)


select * from final_alerts