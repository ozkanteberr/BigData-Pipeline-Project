-- Register Olist DW (Star Schema) tables inside Spark ThriftServer
CREATE DATABASE IF NOT EXISTS olist_dw;
USE olist_dw;

DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/dim_customers';

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/dim_products';

DROP TABLE IF EXISTS dim_sellers;
CREATE TABLE dim_sellers USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/dim_sellers';

DROP TABLE IF EXISTS dim_orders;
CREATE TABLE dim_orders USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/dim_orders';

DROP TABLE IF EXISTS dim_payments;
CREATE TABLE dim_payments USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/dim_payments';

DROP TABLE IF EXISTS fact_order_items;
CREATE TABLE fact_order_items USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/warehouse/fact_order_items';
