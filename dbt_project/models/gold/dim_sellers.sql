{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ source('olist_raw', 'sellers') }}
),
geo AS (
    SELECT * FROM {{ ref('silver_geolocation') }}
)

SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude AS seller_latitude,
    g.longitude AS seller_longitude
FROM sellers s
LEFT JOIN geo g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
