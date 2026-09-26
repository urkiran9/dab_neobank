# Databricks notebook source
# DBTITLE 1,Create parameter widget for insert_timestamp
# Create widget for incremental load parameter
# If null/empty: full load from bronze
# If provided: incremental load for records > insert_timestamp

dbutils.widgets.text("insert_timestamp", "", "Insert Timestamp (YYYY-MM-DD HH:MM:SS)")

# Get the widget value
insert_timestamp_param = dbutils.widgets.get("insert_timestamp").strip()

print(f"Load Type: {'FULL LOAD' if not insert_timestamp_param else 'INCREMENTAL LOAD'}")
if insert_timestamp_param:
    print(f"Filtering records with insert_timestamp > '{insert_timestamp_param}'")

# COMMAND ----------

# DBTITLE 1,Read data from bronze table with conditional filtering
from pyspark.sql import functions as F

# Read from bronze table
bronze_table = "data_modeliing_demo.bronze.scd2"

if insert_timestamp_param:
    # Incremental load: filter records greater than provided timestamp
    bronze_df = spark.table(bronze_table) \
        .filter(F.col("insert_timestamp") > insert_timestamp_param)
    print(f"Incremental load: Reading records with insert_timestamp > '{insert_timestamp_param}'")
else:
    # Full load: read all records
    bronze_df = spark.table(bronze_table)
    print("Full load: Reading all records from bronze table")

# Show record count and sample
record_count = bronze_df.count()
print(f"\nRecords read from bronze: {record_count}")
print("\nSample data:")
display(bronze_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Apply transformations to bronze data
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Apply transformations
transformed_df = bronze_df.select(
    F.col("customer_id"),
    
    # 1. Capitalize first letter of first_name
    F.initcap(F.col("first_name")).alias("first_name"),
    
    # 2. Convert email to lowercase
    F.lower(F.col("email")).alias("email"),
    
    F.col("city"),
    
    # 3. Convert registration_date from string (DD-Mon-YYYY) to actual date
    F.to_date(F.col("registration_date"), "dd-MMM-yyyy").alias("registration_date"),
    
    F.col("last_updated_ts"),
    F.col("insert_timestamp")
)

print("Transformations applied:")
print("  ✓ First name: Capitalized first letter")
print("  ✓ Email: Converted to lowercase")
print("  ✓ Registration date: Converted from string to date type")
print("\nTransformed data sample:")
display(transformed_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Apply data quality checks
from pyspark.sql import functions as F

# Apply data quality checks and add quality flag column
quality_checked_df = transformed_df.withColumn(
    "dq_flag",
    F.when(
        # Check 1: customer_id should not be null
        F.col("customer_id").isNull(), "FAIL: Missing customer_id"
    ).when(
        # Check 2: email should not be null and should contain '@'
        F.col("email").isNull() | ~F.col("email").contains("@"), "FAIL: Invalid email"
    ).when(
        # Check 3: first_name should not be null or empty
        F.col("first_name").isNull() | (F.trim(F.col("first_name")) == ""), "FAIL: Missing first_name"
    ).when(
        # Check 4: registration_date should not be null
        F.col("registration_date").isNull(), "FAIL: Invalid registration_date"
    ).otherwise("PASS")
)

# Filter for valid records only (PASS)
valid_df = quality_checked_df.filter(F.col("dq_flag") == "PASS").drop("dq_flag")

# Show quality check summary
quality_summary = quality_checked_df.groupBy("dq_flag").count().orderBy("dq_flag")
print("Data Quality Check Summary:")
display(quality_summary)

valid_count = valid_df.count()
print(f"\nValid records passing all quality checks: {valid_count}")

# COMMAND ----------

# DBTITLE 1,Write to silver table using SCD Type 1 (MERGE)
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Define silver table name
silver_table = "data_modeliing_demo.silver.scd2"

# Add processing timestamp and SCD Type 2 columns to source data
current_ts = F.current_timestamp()
final_df = valid_df.withColumn("processed_timestamp", current_ts) \
    .withColumn("effective_start_date", current_ts) \
    .withColumn("effective_end_date", F.lit(None).cast("timestamp")) \
    .withColumn("is_current", F.lit(True))

# Check if silver table exists
if spark.catalog.tableExists(silver_table):
    print(f"Silver table '{silver_table}' exists. Performing SCD Type 2 MERGE...")
    
    # Load existing silver table as Delta table
    silver_delta = DeltaTable.forName(spark, silver_table)
    
    # Step 1: MERGE to expire old records that have changes
    silver_delta.alias("target").merge(
        final_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = True"
    ).whenMatchedUpdate(
        condition="""target.first_name != source.first_name OR 
                      target.email != source.email OR 
                      target.city != source.city OR 
                      target.registration_date != source.registration_date""",
        set={
            "effective_end_date": "source.effective_start_date",
            "is_current": "False"
        }
    ).execute()
    
    print("  - Step 1: Expired old versions for changed records")
    
    # Step 2: Insert new versions for ALL source records
    # This includes both: (a) new customers, and (b) updated customers
    # We use INSERT if no current record exists for this customer_id
    silver_delta.alias("target").merge(
        final_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = True"
    ).whenNotMatchedInsertAll().execute()
    
    print("  - Step 2: Inserted new versions (both new and updated records)")
    print("✓ MERGE completed successfully (SCD Type 2)")
    print("  - Changed records: Old version EXPIRED, new version INSERTED")
    print("  - Unchanged records: No action")
    print("  - New records: INSERTED with is_current = True")
else:
    print(f"Silver table '{silver_table}' does not exist. Creating new table...")
    
    # Create new silver table
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
    
    print(f"✓ Silver table '{silver_table}' created successfully")

# Show silver table statistics
print(f"\nSilver table record count:")
silver_count = spark.table(silver_table).count()
current_count = spark.table(silver_table).filter(F.col("is_current") == True).count()
print(f"Total records in silver: {silver_count}")
print(f"Current records (is_current = True): {current_count}")
print(f"Historical records: {silver_count - current_count}")

print("\nSample from silver table (showing current and recent historical):")
display(spark.table(silver_table).orderBy(F.col("customer_id"), F.col("effective_start_date").desc()).limit(20))