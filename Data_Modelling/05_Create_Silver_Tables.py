# Databricks notebook source
# DBTITLE 1,List CSV files in volume
import os

# List files in the volume location
volume_path = "/Volumes/data_modeliing_demo/source/demo_volume/silver_data/"
files = dbutils.fs.ls(volume_path)

# Filter for CSV files
csv_files = [f for f in files if f.name.endswith('.csv')]

print(f"Found {len(csv_files)} CSV files:\n")
for f in csv_files:
    print(f"  - {f.name}")
    
# Store for later use
csv_file_names = [f.name for f in csv_files]

# COMMAND ----------

# DBTITLE 1,Create silver tables from CSV files
# Schema where tables will be created
schema_name = "data_modeliing_demo.silver"
volume_path = "/Volumes/data_modeliing_demo/source/demo_volume/silver_data/"

# Create tables for each CSV file
for csv_file in csv_file_names:
    # Extract table name (remove .csv extension)
    table_name = csv_file.replace('.csv', '')
    full_table_name = f"{schema_name}.{table_name}"
    file_path = f"{volume_path}{csv_file}"
    
    print(f"Creating table: {full_table_name}")
    
    # Read CSV file and write as Delta table
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.write.mode("overwrite").saveAsTable(full_table_name)
    
    # Show row count
    count = spark.table(full_table_name).count()
    print(f"  ✓ Created {full_table_name} with {count:,} rows\n")

print("\n✅ All silver tables created successfully!")

# COMMAND ----------

