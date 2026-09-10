"""
jdbc_parallel_extract.py

Demonstrates how to extract massive relational tables (e.g., 30 Crore / 300 Million rows) 
from an RDBMS like PostgreSQL/SQL Server without locking the source database.

Key Concept: Dynamic Range Partitioning.
We do NOT use a single `SELECT * FROM table`. Instead, we use PySpark's partitioning
mechanisms to open 100 parallel connections, each fetching a specific slice of the Primary Key.
"""
from pyspark.sql import SparkSession
import os

def extract_300m_rows(spark, jdbc_url, table_name, user, password):
    # For a table with 300,000,000 rows, reading with 1 thread will crash.
    # We define the lower bound (e.g., Min ID), upper bound (Max ID), and numPartitions.
    # Spark will automatically split this into 100 queries:
    # Q1: SELECT * FROM table WHERE id >= 1 AND id < 3000000
    # Q2: SELECT * FROM table WHERE id >= 3000000 AND id < 6000000
    
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", "id") \
        .option("lowerBound", "1") \
        .option("upperBound", "300000000") \
        .option("numPartitions", "100") \
        .option("fetchsize", "10000") \
        .load()
        
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("300M_Row_Extract").getOrCreate()
    jdbc_url = "jdbc:postgresql://on-prem-db:5432/enterprise_db"
    
    # Save the output directly into the Bronze layer of ADLS Gen2 as Parquet
    df = extract_300m_rows(spark, jdbc_url, "massive_sales_table", "admin", "secret")
    
    df.write.mode("overwrite").parquet("abfss://bronze@datalake.dfs.core.windows.net/sales/")
    print("300M rows successfully extracted and saved to ADLS Bronze!")
