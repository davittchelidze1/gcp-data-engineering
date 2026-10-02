-- Day 7: query the Parquet Spark wrote, and create a BigQuery-managed Iceberg table.

-- BigLake external table over hive-partitioned Parquet (weekday=0/, weekday=1/, ...)
CREATE OR REPLACE EXTERNAL TABLE day6.biglake_orders
WITH PARTITION COLUMNS (weekday INT64)
WITH CONNECTION `gcp-learning-507108.us-central1.biglake-conn`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gcp-learning-507108-lake/silver/orders_by_weekday/*.parquet'],
  hive_partition_uri_prefix = 'gs://gcp-learning-507108-lake/silver/orders_by_weekday'
);

SELECT weekday, orders, ROUND(revenue, 0) AS revenue
FROM day6.biglake_orders
ORDER BY weekday;

-- BigQuery-managed Iceberg: BigQuery writes open-format files to your bucket
CREATE OR REPLACE TABLE day6.iceberg_orders (
  order_id INT64, weekday INT64, items INT64, unit_price FLOAT64, total_value FLOAT64
)
WITH CONNECTION `gcp-learning-507108.us-central1.biglake-conn`
OPTIONS (
  file_format  = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri  = 'gs://gcp-learning-507108-lake/iceberg/orders'
);

INSERT INTO day6.iceberg_orders
SELECT order_id, weekday, items, unit_price, total_value
FROM day6.ml_orders
LIMIT 500;

-- on GCS: iceberg/orders/metadata/v0.metadata.json + iceberg/orders/data/*.parquet
SELECT COUNT(*) AS n, ROUND(AVG(total_value), 2) AS avg_value FROM day6.iceberg_orders;
