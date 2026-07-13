{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ source('olist_raw', 'products') }}
),
translation AS (
    SELECT * FROM {{ source('olist_raw', 'product_category_name_translation') }}
)

SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM products p
LEFT JOIN translation t ON p.product_category_name = t.product_category_name
