"""
scd_type_1_upsert.py

This script simulates a complete Slowly Changing Dimension (SCD) Type 1 UPSERT process.
SCD Type 1 means we completely overwrite old data with new data (no historical tracking).

It demonstrates:
1. Creating a Mock Silver Delta Table (Target)
2. Receiving a Mock CDC Stream (Updates & Inserts)
3. Merging the CDC into the Silver Table
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable
import shutil
import os

def setup_spark():
    # Initialize Spark with Delta Lake capabilities
    return SparkSession.builder.appName("UPSERT_Simulation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()

def simulate_upsert():
    spark = setup_spark()
    silver_table_path = "/tmp/delta/silver_customers"
    
    # Cleanup previous runs
    if os.path.exists(silver_table_path):
        shutil.rmtree(silver_table_path)

    # 1. CREATE INITIAL SILVER TABLE (Day 1)
    print("\n--- Day 1: Initial Load ---")
    initial_data = [
        (1, "Alice", "New York"),
        (2, "Bob", "Los Angeles")
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True)
    ])
    
    df_initial = spark.createDataFrame(initial_data, schema)
    df_initial.write.format("delta").save(silver_table_path)
    print("Initial Silver Table:")
    spark.read.format("delta").load(silver_table_path).show()

    # 2. RECEIVE CDC STREAM (Day 2)
    print("\n--- Day 2: CDC Updates Arrive ---")
    # Alice moves to Boston (UPDATE), Charlie signs up (INSERT)
    cdc_data = [
        (1, "Alice", "Boston"),     # UPDATE
        (3, "Charlie", "Chicago")   # INSERT
    ]
    df_cdc = spark.createDataFrame(cdc_data, schema)
    print("Incoming CDC Data:")
    df_cdc.show()

    # 3. PERFORM THE UPSERT (MERGE INTO)
    print("\n--- Executing UPSERT (MERGE INTO) ---")
    silver_table = DeltaTable.forPath(spark, silver_table_path)
    
    silver_table.alias("target").merge(
        df_cdc.alias("updates"),
        "target.id = updates.id" # The Primary Key Match
    ).whenMatchedUpdate(set = {
        "name": "updates.name",
        "city": "updates.city"
    }).whenNotMatchedInsert(values = {
        "id": "updates.id",
        "name": "updates.name",
        "city": "updates.city"
    }).execute()

    # 4. VIEW FINAL RESULTS
    print("\n--- Final Silver Table State (Notice Alice's city changed, and Charlie was added) ---")
    spark.read.format("delta").load(silver_table_path).orderBy("id").show()

if __name__ == "__main__":
    simulate_upsert()
