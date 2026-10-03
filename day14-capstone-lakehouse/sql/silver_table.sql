-- Silver as a BigLake table over the Parquet Spark writes, partitioned by ingest date.
-- New partitions are visible the moment Spark writes them - there is no load step.
-- require_hive_partition_filter: a query that would scan all of silver is refused.
CREATE OR REPLACE EXTERNAL TABLE platform.silver_orders
WITH PARTITION COLUMNS (ingest_date DATE)
WITH CONNECTION `gcp-learning-507108.us-central1.biglake-conn`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gcp-learning-507108-silver/orders/*.parquet'],
  hive_partition_uri_prefix = 'gs://gcp-learning-507108-silver/orders',
  require_hive_partition_filter = true
);
