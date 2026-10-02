    """Day 7 - a normal Spark job, unchanged, running on Dataproc Serverless.

Demonstrates:
  * BigQuery Storage Read API (direct read, no GCS staging)
  * writing Parquet to GCS as the 'silver' lakehouse layer
  * indirect write back to BigQuery via a temp GCS bucket
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT = "gcp-learning-507108"
LAKE = "gs://%s-lake" % PROJECT
TEMP = "%s-dataproc" % PROJECT

spark = (SparkSession.builder
         .appName("day7-orders-rollup")
         .config("temporaryGcsBucket", TEMP)
         .getOrCreate())

print("Spark version:", spark.version)

# ---- read straight from BigQuery (Storage Read API, no export step) ----
orders = (spark.read.format("bigquery")
          .option("table", "%s.day6.ml_orders" % PROJECT)
          .load())
print("rows read from BigQuery:", orders.count())
orders.printSchema()

# ---- ordinary Spark transformation ----
rollup = (orders
          .withColumn("revenue", F.col("items") * F.col("unit_price"))
          .groupBy("weekday")
          .agg(F.count("*").alias("orders"),
               F.round(F.sum("revenue"), 2).alias("revenue"),
               F.round(F.avg("total_value"), 2).alias("avg_order_value"),
               F.max("total_value").alias("max_order_value"))
          .orderBy("weekday"))

rollup.show()

# ---- silver layer: partitioned Parquet on GCS ----
(rollup.write.mode("overwrite")
 .partitionBy("weekday")
 .parquet("%s/silver/orders_by_weekday" % LAKE))
print("wrote Parquet to", "%s/silver/orders_by_weekday" % LAKE)

# ---- gold layer: back into BigQuery ----
(rollup.write.format("bigquery")
 .option("table", "%s.day6.spark_orders_rollup" % PROJECT)
 .option("writeMethod", "indirect")
 .mode("overwrite")
 .save())
print("wrote BigQuery table day6.spark_orders_rollup")

spark.stop()
