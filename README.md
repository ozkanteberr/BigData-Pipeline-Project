# Big Data Analytics Pipeline - Olist E-Commerce

A Spark-based analytics pipeline for the Olist Brazilian E-Commerce public dataset. The current phase ingests the nine CSV tables, converts them to Parquet, stores them in HDFS, and exposes them to Apache Superset through Spark SQL tables.

Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Architecture

```text
[Olist CSV files]
        |
        v
[Apache Spark ingestion job]
        |
        v
[Parquet tables on HDFS]
        |
        v
[Spark SQL / ThriftServer]
        |
        v
[Apache Superset charts]
```

## Repository Layout

```text
processing/      Spark ingestion jobs
visualization/   Spark SQL table registration helpers
scripts/         Dataset download and environment setup helpers
docker/          Optional local Docker Compose stack
reports/         Project report and analysis notes
```

## Prerequisites

- Docker and Docker Compose, if using the included local stack
- Python 3.11+, if running scripts locally
- Kaggle API token at `~/.kaggle/kaggle.json`

Install Python dependencies locally with:

```bash
pip install -r requirements.txt
```

## Quick Start With Docker

Create the shared Docker network:

```bash
# Linux / macOS
bash scripts/setup_network.sh

# Windows PowerShell
.\scripts\setup_network.ps1
```

Start HDFS, Spark, and Superset:

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-spark.yml up -d
docker compose -f docker/docker-compose-superset.yml up -d
```

Useful URLs:

| Service       | URL                   | Credentials   |
|---------------|-----------------------|---------------|
| HDFS NameNode | http://localhost:9870 |               |
| Spark Master  | http://localhost:8080 |               |
| Superset      | http://localhost:8088 | admin / admin |

## Download The Dataset

```bash
python scripts/download_dataset.py
```

This writes CSV files to `data/raw/`. The directory is ignored by git.

## Convert CSV To Parquet

Local output example:

```bash
python processing/analysis.py --input data/raw --output data/processed/parquet
```

HDFS output example from the dev container or Spark container:

```bash
spark-submit processing/analysis.py \
  --input /app/data/raw \
  --output hdfs://namenode:9000/user/olist/parquet
```

The job writes one Parquet dataset per CSV table, using table names such as `orders`, `customers`, and `order_items`.

## Register Tables For Superset

After the Parquet datasets are written, register them in Spark SQL:

```bash
spark-submit visualization/register_tables.py \
  --input hdfs://namenode:9000/user/olist/parquet \
  --database olist
```

Then connect Superset to Spark ThriftServer with a SQLAlchemy URI similar to:

```text
hive://spark-thriftserver:10000/olist
```

## Bootstrap Superset Dashboard

Once the tables are registered and all three Docker stacks are running, create the
database connection, datasets, charts, and a starter dashboard automatically:

```bash
pip install requests   # if not already installed
python scripts/bootstrap_superset.py
```

This creates the **Olist E-Commerce Overview** dashboard with five charts:
orders over time, payment method distribution, customer geography, revenue by
category, and order status breakdown.

> **Note:** The script waits for Superset to become healthy before proceeding.
> If Superset is running on a non-default URL, pass `--superset-url`.

## Stop The Stack

```bash
docker compose -f docker/docker-compose-superset.yml down
docker compose -f docker/docker-compose-spark.yml down
docker compose -f docker/docker-compose-hdfs.yml down
```

## Current Phase

- Ingest all Olist CSV files.
- Convert them to Parquet with Spark.
- Store outputs in HDFS.
- Register queryable Spark SQL tables.
- Build basic Superset charts from the registered tables.
