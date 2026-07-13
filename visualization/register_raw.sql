-- Register Olist Raw tables inside Spark ThriftServer
CREATE DATABASE IF NOT EXISTS olist;
USE olist;

DROP TABLE IF EXISTS customers;
CREATE TABLE customers USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/customers';

DROP TABLE IF EXISTS geolocation;
CREATE TABLE geolocation USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/geolocation';

DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/order_items';

DROP TABLE IF EXISTS order_payments;
CREATE TABLE order_payments USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/order_payments';

DROP TABLE IF EXISTS order_reviews;
CREATE TABLE order_reviews USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/order_reviews';

DROP TABLE IF EXISTS orders;
CREATE TABLE orders USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/orders';

DROP TABLE IF EXISTS products;
CREATE TABLE products USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/products';

DROP TABLE IF EXISTS sellers;
CREATE TABLE sellers USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/sellers';

DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE product_category_name_translation USING PARQUET LOCATION 'hdfs://namenode:9000/user/olist/parquet/product_category_name_translation';
