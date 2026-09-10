# Medallion Architecture (Lakehouse)

The Medallion Architecture logically organizes data in a Lakehouse (like ADLS Gen2 with Databricks).

## 1. Bronze Layer (Raw Landing)
* **Purpose:** Write data as-is into ADLS Gen2. Keep an exact historical replica of the source.
* **Formats:** Parquet, Delta, or Raw JSON/CSV.
* **Path Pattern:** `/bronze/{source_system}/{entity}/ingest_year=YYYY/ingest_month=MM/`

## 2. Silver Layer (Cleansed & Conformed)
* **Purpose:** Cleanse data, enforce data types, apply deduplication, and execute Change Data Capture (CDC).
* **Format:** Delta Lake.
* **Mechanics:** Use Delta Lake `MERGE INTO` operations for idempotent updates (UPSERTs).

## 3. Gold Layer (Business / Consumption)
* **Purpose:** Aggregate data into dimensional star schemas (Facts and Dimensions) ready for reporting.
* **Format:** Delta Lake.
* **Serving:** Serve to Power BI DirectLake, Azure Synapse Serverless SQL, or Snowflake.
