"""
Convert the Olist CSV dataset into Parquet tables with Apache Spark.

Examples:
  python processing/analysis.py --input data/raw --output data/processed/parquet

  spark-submit processing/analysis.py \
    --input /app/data/raw \
    --output hdfs://namenode:9000/user/olist/parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


DEFAULT_INPUT_DIR = "data/raw"
DEFAULT_OUTPUT_DIR = "hdfs://namenode:9000/user/olist/parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Olist CSV files and write one Parquet dataset per table."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing Olist CSV files. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Parquet output base path. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master URL, for example spark://spark-master:7077.",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=None,
        help="Optionally reduce each output table to this number of files.",
    )
    return parser.parse_args()


def build_spark(master: str | None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("olist-csv-to-parquet")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def table_name_from_csv(csv_path: Path) -> str:
    name = csv_path.stem
    if name.startswith("olist_"):
        name = name[len("olist_"):]
    if name.endswith("_dataset"):
        name = name[:-len("_dataset")]
    return name


def read_csv(spark: SparkSession, csv_path: Path) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(str(csv_path))
    )


def convert_table(
    spark: SparkSession,
    csv_path: Path,
    output_base_path: str,
    coalesce: int | None,
) -> tuple[str, int]:
    table_name = table_name_from_csv(csv_path)
    output_path = f"{output_base_path.rstrip('/')}/{table_name}"

    frame = read_csv(spark, csv_path)
    row_count = frame.count()

    writer_frame = frame.coalesce(coalesce) if coalesce else frame
    writer_frame.write.mode("overwrite").parquet(output_path)

    print(f"OK: {csv_path.name} -> {output_path} ({row_count:,} rows)")
    return table_name, row_count


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input)
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir.resolve()}")

    spark = build_spark(args.master)
    try:
        results = [
            convert_table(spark, csv_path, args.output, args.coalesce)
            for csv_path in csv_files
        ]
    finally:
        spark.stop()

    print("\nConverted tables:")
    for table_name, row_count in results:
        print(f"  {table_name:<35} {row_count:>10,} rows")


if __name__ == "__main__":
    main()
