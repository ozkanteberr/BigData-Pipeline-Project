from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-duplicates").getOrCreate()

tables = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_name_translation"
]

input_base = "hdfs://namenode:9000/user/olist/parquet"

print("--- Duplicate Checking ---")
for t in tables:
    df = spark.read.parquet(f"{input_base}/{t}")
    total_count = df.count()
    distinct_count = df.distinct().count()
    duplicate_count = total_count - distinct_count
    print(f"Table: {t:<35} | Total: {total_count:<10} | Distinct: {distinct_count:<10} | Duplicates: {duplicate_count}")

spark.stop()
