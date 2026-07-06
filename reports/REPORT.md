# Olist Big Data Pipeline Report

## Phase 1 Scope

This phase focuses on building a reproducible ingestion and visualization path for the Olist Brazilian E-Commerce dataset.

## Data Sources

The source dataset contains nine CSV tables from Kaggle:

- customers
- geolocation
- order_items
- order_payments
- order_reviews
- orders
- products
- sellers
- product_category_name_translation

## Pipeline Summary

1. Download the raw CSV files with `scripts/download_dataset.py`.
2. Convert each CSV file to a Parquet dataset with `processing/analysis.py`.
3. Store the Parquet outputs under `hdfs://namenode:9000/user/olist/parquet`.
4. Register the Parquet datasets as Spark SQL tables with `visualization/register_tables.py`.
5. Connect Superset to Spark ThriftServer and create exploratory charts.

## Validation Checklist

- Raw CSV files are present under `data/raw/`.
- Each expected table has a matching Parquet output directory.
- Spark SQL can query the registered `olist` database tables.
- Superset can connect to `hive://spark-thriftserver:10000/olist`.
- At least a few simple charts are created for orders, payments, and customer geography.

## Notes And Findings

To be completed after the first full pipeline run.
