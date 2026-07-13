"""
Transform Olist raw Parquet tables into a cleaned Star Schema (Fact and Dimension tables).

Examples:
  spark-submit processing/transform.py \
    --input hdfs://namenode:9000/user/olist/parquet \
    --output hdfs://namenode:9000/user/olist/warehouse
"""

from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, datediff, first, lit, expr


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Olist tables into a cleaned Star Schema."
    )
    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/user/olist/parquet",
        help="Input base path of raw Parquet tables.",
    )
    parser.add_argument(
        "--output",
        default="hdfs://namenode:9000/user/olist/warehouse",
        help="Output base path for Star Schema tables.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_base = args.input.rstrip("/")
    output_base = args.output.rstrip("/")

    spark = SparkSession.builder.appName("olist-star-schema-transform").getOrCreate()

    try:
        print("Reading raw tables...")
        df_customers = spark.read.parquet(f"{input_base}/customers")
        df_geolocation = spark.read.parquet(f"{input_base}/geolocation")
        df_order_items = spark.read.parquet(f"{input_base}/order_items")
        df_order_payments = spark.read.parquet(f"{input_base}/order_payments")
        df_order_reviews = spark.read.parquet(f"{input_base}/order_reviews")
        df_orders = spark.read.parquet(f"{input_base}/orders")
        df_products = spark.read.parquet(f"{input_base}/products")
        df_sellers = spark.read.parquet(f"{input_base}/sellers")
        df_translation = spark.read.parquet(f"{input_base}/product_category_name_translation")

        # ── 1. Geolocation De-duplication ────────────────────────────
        print("Cleaning and de-duplicating geolocation data...")
        df_geo_clean = df_geolocation.groupBy("geolocation_zip_code_prefix").agg(
            avg("geolocation_lat").alias("latitude"),
            avg("geolocation_lng").alias("longitude"),
            first("geolocation_city").alias("city"),
            first("geolocation_state").alias("state")
        )

        # ── 2. Dimension: dim_customers ────────────────────────────────
        print("Building dim_customers...")
        dim_customers = df_customers.join(
            df_geo_clean,
            df_customers.customer_zip_code_prefix == df_geo_clean.geolocation_zip_code_prefix,
            "left"
        ).select(
            df_customers.customer_id,
            df_customers.customer_unique_id,
            df_customers.customer_zip_code_prefix,
            df_customers.customer_city,
            df_customers.customer_state,
            col("latitude").alias("customer_latitude"),
            col("longitude").alias("customer_longitude")
        )

        # ── 3. Dimension: dim_products ─────────────────────────────────
        print("Building dim_products...")
        dim_products = df_products.join(
            df_translation,
            "product_category_name",
            "left"
        ).select(
            df_products.product_id,
            df_products.product_category_name,
            col("product_category_name_english").alias("product_category_name_english"),
            df_products.product_name_lenght,
            df_products.product_description_lenght,
            df_products.product_photos_qty,
            df_products.product_weight_g,
            df_products.product_length_cm,
            df_products.product_height_cm,
            df_products.product_width_cm
        )

        # ── 4. Dimension: dim_sellers ──────────────────────────────────
        print("Building dim_sellers...")
        dim_sellers = df_sellers.join(
            df_geo_clean,
            df_sellers.seller_zip_code_prefix == df_geo_clean.geolocation_zip_code_prefix,
            "left"
        ).select(
            df_sellers.seller_id,
            df_sellers.seller_zip_code_prefix,
            df_sellers.seller_city,
            df_sellers.seller_state,
            col("latitude").alias("seller_latitude"),
            col("longitude").alias("seller_longitude")
        )

        # ── 5. Dimension: dim_orders ───────────────────────────────────
        print("Building dim_orders...")
        dim_orders = df_orders.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        )

        # ── 6. Dimension: dim_payments ─────────────────────────────────
        print("Building dim_payments...")
        dim_payments = df_order_payments.select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        )

        # ── 7. Fact Table: fact_order_items ────────────────────────────
        print("Building fact_order_items...")
        
        # Calculate avg review score per order (since an order can have multiple reviews)
        df_reviews_avg = df_order_reviews.groupBy("order_id").agg(
            avg("review_score").alias("review_score")
        )

        # Join items with orders (for dates) and reviews
        fact_order_items = df_order_items.join(
            df_orders,
            "order_id",
            "inner"
        ).join(
            df_reviews_avg,
            "order_id",
            "left"
        ).select(
            expr("concat(order_id, '_', cast(order_item_id as string))").alias("order_item_key"),
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            "price",
            "freight_value",
            (col("price") + col("freight_value")).alias("item_revenue"),
            datediff("order_delivered_customer_date", "order_purchase_timestamp").alias("delivery_time_days"),
            "review_score"
        )

        # ── 8. Write Star Schema tables ────────────────────────────────
        print(f"Writing tables to HDFS warehouse path: {output_base}")
        
        dim_customers.write.mode("overwrite").parquet(f"{output_base}/dim_customers")
        dim_products.write.mode("overwrite").parquet(f"{output_base}/dim_products")
        dim_sellers.write.mode("overwrite").parquet(f"{output_base}/dim_sellers")
        dim_orders.write.mode("overwrite").parquet(f"{output_base}/dim_orders")
        dim_payments.write.mode("overwrite").parquet(f"{output_base}/dim_payments")
        fact_order_items.write.mode("overwrite").parquet(f"{output_base}/fact_order_items")

        print("\nAll dimension and fact tables written successfully.")
        print(f"  dim_customers:    {dim_customers.count():,} rows")
        print(f"  dim_products:     {dim_products.count():,} rows")
        print(f"  dim_sellers:      {dim_sellers.count():,} rows")
        print(f"  dim_orders:       {dim_orders.count():,} rows")
        print(f"  dim_payments:     {dim_payments.count():,} rows")
        print(f"  fact_order_items: {fact_order_items.count():,} rows")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
