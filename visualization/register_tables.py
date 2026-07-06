"""
Register Parquet outputs as Spark SQL tables for Superset exploration.

Run this after processing/analysis.py writes the Parquet datasets.

Example:
  spark-submit visualization/register_tables.py \
    --input hdfs://namenode:9000/user/olist/parquet \
    --database olist
"""

from __future__ import annotations

import argparse
import re

from pyspark.sql import SparkSession


DEFAULT_INPUT_DIR = "hdfs://namenode:9000/user/olist/parquet"
DEFAULT_DATABASE = "olist"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Spark SQL tables over generated Olist Parquet datasets."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_DIR,
        help=f"Base Parquet path. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        help=f"Spark SQL database name. Default: {DEFAULT_DATABASE}",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master URL, for example spark://spark-master:7077.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Optional table list. Defaults to the standard Olist table set.",
    )
    return parser.parse_args()


def build_spark(master: str | None) -> SparkSession:
    builder = SparkSession.builder.appName("olist-register-parquet-tables")
    if master:
        builder = builder.master(master)
    return builder.enableHiveSupport().getOrCreate()


def default_tables() -> list[str]:
    return [
        "customers",
        "geolocation",
        "order_items",
        "order_payments",
        "order_reviews",
        "orders",
        "products",
        "sellers",
        "product_category_name_translation",
    ]


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(f"Invalid Spark SQL identifier: {identifier}")
    return identifier


def sql_string(value: str) -> str:
    return value.replace("'", "''")


def main() -> None:
    args = parse_args()
    database = validate_identifier(args.database)
    tables = [validate_identifier(table) for table in (args.tables or default_tables())]
    input_base_path = args.input.rstrip("/")

    spark = build_spark(args.master)
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
