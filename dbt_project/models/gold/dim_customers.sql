{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ source('olist_raw', 'customers') }}
),
geo AS (
    SELECT * FROM {{ ref('silver_geolocation') }}
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.latitude AS customer_latitude,
    g.longitude AS customer_longitude
FROM customers c
LEFT JOIN geo g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
