SELECT
    NULLIF(raw_data -> 'data' ->> 'aqi', '-'):: INT AS                        aqi_index,    -- real time air quality information
    NULLIF(raw_data -> 'data' ->> 'idx', '-'):: INT AS                        station_id,
    (raw_data -> 'data' -> 'city' ->> 'name'):: VARCHAR AS                    station_name,
    NULLIF(raw_data -> 'data' -> 'city' -> 'geo' ->> 0, '-'):: FLOAT AS       latitude,
    NULLIF(raw_data -> 'data' -> 'city' -> 'geo' ->> 1, '-'):: FLOAT AS       longitude,
    (raw_data -> 'data' -> 'time' ->> 'iso'):: TIMESTAMPTZ AS                 api_measurement_time,
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'pm25' ->> 'v', '-'):: FLOAT AS    pm25,
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'pm10' ->> 'v', '-'):: FLOAT AS    pm10,
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'no2' ->> 'v', '-'):: FLOAT AS     no2,
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'o3' ->> 'v', '-'):: FLOAT AS      o3,
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'so2' ->> 'v', '-'):: FLOAT AS     so2,    -- low priority (only for ports or industrial areas)
    NULLIF(raw_data -> 'data' -> 'iaqi' -> 'co' ->> 'v', '-'):: FLOAT AS      co,     -- low priority (only for ports or industrial areas)
    ingestion_time

FROM "air_quality_db"."public"."london_raw_data"