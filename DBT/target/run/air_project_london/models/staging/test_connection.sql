
  
    

  create  table "air_quality_db"."public"."test_connection__dbt_tmp"
  
  
    as
  
  (
    

WITH raw_data AS (
    -- Leemos de la fuente definida arriba
    SELECT * FROM "air_quality_db"."public"."london_raw_data"
)

SELECT
    id,
    ingestion_time,
    -- Extraemos un par de campos del JSON para probar
    raw_data->>'station_name' as station,
    raw_data->>'parameter' as pollutant,
    (raw_data->>'value')::numeric as value
FROM raw_data
  );
  