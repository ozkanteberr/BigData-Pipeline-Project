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

### Architecture Decisions

- **Separate Docker Compose files**: HDFS, Spark, and Superset are split into independent compose
  files so each layer can be started, stopped, and scaled independently. All services share the
  external `bigdata-net` Docker network.
- **Spark ThriftServer in local mode**: The ThriftServer runs `--master local[2]` instead of
  connecting to the Spark cluster. This avoids resource contention for the always-on JDBC endpoint
  while the cluster workers remain available for batch Spark-submit jobs.
- **Snappy compression for Parquet**: Selected for its balance of compression ratio and read speed,
  which suits interactive Superset queries.
- **Hive metastore not required**: `CREATE TABLE ... USING PARQUET LOCATION` avoids the need for a
  standalone Hive metastore. Table registrations must be re-run if the ThriftServer restarts.

### Data Observations

- The Olist dataset contains **9 CSV tables**, totalling roughly 45 MB of raw data.
- `orders` is the central fact table (~100 k rows) with timestamps spanning 2016-09-15 to
  2018-10-17.
- `geolocation` is the largest table (~1 M rows) and contains latitude/longitude pairs for
  Brazilian zip-code prefixes.
- `product_category_name_translation` is a small lookup table mapping Portuguese category names to
  English.

### Known Limitations (Phase 1)

- Superset dashboard charts are bootstrapped via the REST API; manual adjustments may be needed for
  optimal layout and formatting.
- No incremental or streaming ingestion — the pipeline does a full overwrite on each run.
- MinIO (S3-compatible storage) is provisioned but not yet integrated into the pipeline. It is
  reserved for Phase 2 when object-storage-based workflows are planned.
- The ThriftServer does not persist its metastore; table registrations are lost on container restart
  and must be re-applied with `register_tables.py`.
