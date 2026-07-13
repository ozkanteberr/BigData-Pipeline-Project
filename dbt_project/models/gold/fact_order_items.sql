{{ config(materialized='table') }}

WITH order_items AS (
    SELECT * FROM {{ source('olist_raw', 'order_items') }}
),
orders AS (
    SELECT * FROM {{ ref('dim_orders') }}
),
reviews AS (
    SELECT
        order_id,
        AVG(review_score) AS review_score
    FROM {{ source('olist_raw', 'order_reviews') }}
    GROUP BY order_id
)

SELECT
    CONCAT(i.order_id, '_', CAST(i.order_item_id AS STRING)) AS order_item_key,
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS item_revenue,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_time_days,
    r.review_score
FROM order_items i
JOIN orders o ON i.order_id = o.order_id
LEFT JOIN reviews r ON i.order_id = r.order_id
