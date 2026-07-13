from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_medallion_pipeline',
    default_args=default_args,
    description='A data pipeline for Olist e-commerce using Spark and dbt',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['olist', 'dbt', 'spark'],
) as dag:

    # Task 1: Data Ingestion (Triggering Spark Job via Docker)
    # We use a mocked ingestion echo here for demonstration, 
    # but in reality, it would run: docker exec spark-master spark-submit ...
    ingestion_task = BashOperator(
        task_id='trigger_spark_ingestion',
        bash_command='echo "Ingesting CSVs to HDFS Bronze Parquet..." && sleep 5',
    )

    # Task 2: dbt Transformation (Silver and Gold Layers)
    # Runs the dbt project using the dbt-spark adapter connecting to ThriftServer
    dbt_transform_task = BashOperator(
        task_id='trigger_dbt_transform',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    # Task 3: Refresh BI Dashboards
    # Normally this could clear cache via Superset API, we simulate the hook here.
    refresh_bi_task = BashOperator(
        task_id='refresh_superset_dashboards',
        bash_command='echo "Refreshing Superset BI Dashboards cache..."',
    )

    # Define DAG Dependencies
    ingestion_task >> dbt_transform_task >> refresh_bi_task
