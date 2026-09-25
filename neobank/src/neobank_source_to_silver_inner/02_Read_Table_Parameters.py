# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ==========================================
# 1️⃣ Create Widget
# ==========================================

default_metadata = """{
    "table_id": "1",
    "table_name": "customers",
    "source_system": "sqlserver",
    "source_schema": "banking",
    "source_table": "customers",
    "source_path": "",
    "target_layer": "silver",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "",
    "active_flag": "True",
    "load_order": "1",
    "created_at": "2026-09-23 02:03:03.214555"
  }"""
dbutils.widgets.text("table_metadata", default_metadata)
table_metadata_str = dbutils.widgets.get("table_metadata")

print(f"Table Metadata Passed: {table_metadata_str}")

if not table_metadata_str:
    raise ValueError("table_metadata widget cannot be empty")

import ast
table_metadata = ast.literal_eval(table_metadata_str)

table_id = table_metadata.get("table_id")
if not table_id:
    raise ValueError("table_id not found in table_metadata")

table_id = int(table_id)


# ==========================================
# 2️⃣ Read Table Parameters
# ==========================================

params_df = (
    spark.table("banking.metadata.table_parameters")
         .filter(f"table_id = {table_id}")
)

# ==========================================
# 3️⃣ Convert to Single JSON Object
# ==========================================

rows = params_df.select("parameter_name", "parameter_value").collect()

parameters_dict = {
    row.parameter_name: row.parameter_value
    for row in rows
}

print("Parameters JSON Object:")
print(parameters_dict)


# ==========================================
# 4️⃣ Set Databricks Task Value
# ==========================================

dbutils.jobs.taskValues.set(
    key="table_parameters",
    value=parameters_dict
)

print("Task value 'table_parameters' has been set.")