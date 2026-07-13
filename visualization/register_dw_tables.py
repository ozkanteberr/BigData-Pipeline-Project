"""
Register Star Schema Parquet datasets as Spark SQL tables for DW analytics.

Example:
  spark-submit visualization/register_dw_tables.py \
    --input hdfs://namenode:9000/user/olist/warehouse \
    --database olist_dw
"""

from __future__ import annotations

import argparse
import re

from pyspark.sql import SparkSession

DEFAULT_INPUT_DIR = "hdfs://namenode:9000/user/olist/warehouse"
DEFAULT_DATABASE = "olist_dw"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Spark SQL tables over generated Olist Star Schema datasets."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_DIR,
        help=f"Base warehouse path. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        help=f"Spark SQL database name. Default: {DEFAULT_DATABASE}",
    )
    return parser.parse_args()


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(f"Invalid Spark SQL identifier: {identifier}")
    return identifier


def sql_string(value: str) -> str:
    return value.replace("'", "''")


def main() -> None:
    args = parse_args()
    database = validate_identifier(args.database)
    input_base_path = args.input.rstrip("/")

    tables = [
        "dim_customers",
        "dim_products",
        "dim_sellers",
        "dim_orders",
        "dim_payments",
        "fact_order_items",
    ]

    spark = SparkSession.builder.appName("olist-register-dw-tables").enableHiveSupport().getOrCreate()
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        spark.sql(f"USE {database}")

        for table_name in tables:
            path = f"{input_base_path}/{table_name}"
            spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
            spark.sql(
                f"CREATE TABLE {database}.{table_name} "
                f"USING PARQUET LOCATION '{sql_string(path)}'"
            )
            print(f"OK: registered {database}.{table_name} from {path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
