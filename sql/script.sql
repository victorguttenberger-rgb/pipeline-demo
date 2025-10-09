DECLARE max_existing_ingest DATETIME;
DECLARE max_new_ingest DATETIME;

SET max_existing_ingest = (
  SELECT MAX(ingest_datetime)
  FROM `INTEGRATION.integracion_prueba_tecnica`
);

SET max_new_ingest = (
  SELECT MAX(ingest_datetime)
  FROM `SANDBOX_PIPELINE_DEMO.countries_from_API`
);

IF max_new_ingest > max_existing_ingest THEN
CREATE OR REPLACE TABLE `INTEGRATION.integracion_prueba_tecnica` AS
SELECT
  cca3
  , name
  , languages
  , population
  , continents
  , ingest_datetime
  , CURRENT_DATETIME() AS transform_datetime
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY cca3 ORDER BY ingest_datetime DESC) AS recent
  FROM `SANDBOX_PIPELINE_DEMO.countries_from_API`
)
WHERE recent = 1;
END IF;