{{ config(materialized='table') }}

WITH raw_geo AS (
    SELECT * FROM {{ source('olist_raw', 'geolocation') }}
)

SELECT
    geolocation_zip_code_prefix,
    AVG(geolocation_lat) AS latitude,
    AVG(geolocation_lng) AS longitude,
    FIRST(geolocation_city) AS city,
    FIRST(geolocation_state) AS state
FROM raw_geo
GROUP BY geolocation_zip_code_prefix
