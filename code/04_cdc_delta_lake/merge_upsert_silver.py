"""
merge_upsert_silver.py

Demonstrates Change Data Capture (CDC) processing using Delta Lake in PySpark.
When moving data from the Bronze layer (Raw Append) to the Silver layer (Cleansed), 
you must apply updates and inserts (UPSERTs) seamlessly.
"""
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def merge_cdc_to_silver(spark, bronze_cdc_df, silver_table_path, primary_key):
    """
    Merges Change Data Capture (CDC) updates into a Silver Delta Lake table.
    This operation is Idempotent (safe to retry if the pipeline fails mid-way).
    """
    # 1. Check if the Silver table already exists
    if not DeltaTable.isDeltaTable(spark, silver_table_path):
        print(f"Silver table doesn't exist at {silver_table_path}. Performing initial load...")
        bronze_cdc_df.write.format("delta").mode("overwrite").save(silver_table_path)
        return

    # 2. Load the existing Silver Delta Table
    silver_table = DeltaTable.forPath(spark, silver_table_path)

    # 3. Execute the UPSERT (Update existing rows, Insert new rows)
    print("Merging Bronze CDC updates into Silver table...")
    silver_table.alias("target") \
        .merge(
            bronze_cdc_df.alias("updates"),
            f"target.{primary_key} = updates.{primary_key}"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
    print("Merge complete!")

if __name__ == "__main__":
    # Ensure Spark is launched with Delta Lake extensions
    spark = SparkSession.builder.appName("CDC_Delta_Merge") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    # In practice, you would load your CDC stream (e.g., from Event Hubs or newly arrived Bronze files)
    # bronze_cdc_df = spark.read.parquet("abfss://bronze@.../new_changes/")
    # merge_cdc_to_silver(spark, bronze_cdc_df, "abfss://silver@.../customers/", "customer_id")
