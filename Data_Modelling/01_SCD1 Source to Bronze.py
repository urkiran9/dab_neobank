# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Configuration
source_path = "/Volumes/data_modeliing_demo/source/demo_volume/scd1/input_files/"
target_table = "data_modeliing_demo.bronze.scd1"
checkpoint_path = "/Volumes/data_modeliing_demo/source/demo_volume/scd1/checkpoints/"
schema_location = "/Volumes/data_modeliing_demo/source/demo_volume/scd1/schema/"

# Read CSV files using Auto Loader in batch mode
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)  # Required for Auto Loader
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
    .withColumn("insert_timestamp", current_timestamp())
)

# Write to bronze table using batch mode (trigger availableNow)
query = (df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

# Wait for the batch to complete
query.awaitTermination()

print(f"✓ Data loaded successfully to {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_rows FROM data_modeliing_demo.bronze.scd1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_modeliing_demo.bronze.scd1 LIMIT 10;