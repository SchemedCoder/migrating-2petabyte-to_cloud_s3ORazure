# Understanding UPSERTS and Change Data Capture (CDC)

When migrating data, extracting the initial bulk payload is only step one. Step two is keeping the target system synchronized with ongoing changes at the source.

## What is CDC? (Change Data Capture)
CDC is the process of identifying and capturing changes made to data in a database (INSERTs, UPDATEs, DELETEs) and delivering those changes in real-time to a downstream system (like a Data Lake).
* **Log-Based CDC:** The industry standard. Tools read the database transaction logs (WAL/Binlog). This has zero performance impact on the database's compute.
* **Query-Based CDC:** A pipeline runs `SELECT * FROM table WHERE modified_date > ?` every 5 minutes. This is easier to setup but puts a heavy load on the source database.

## What is an UPSERT?
UPSERT is a portmanteau of **UP**date and in**SERT**. 
When a CDC stream sends you a record, you don't know if that record already exists in your Data Lake. 
* If it **exists**, you want to UPDATE the existing row.
* If it **does not exist**, you want to INSERT it as a new row.

## Why is it critical for Idempotency?
Idempotency means that if your data pipeline fails mid-way and you restart it, it will not create duplicate data.
If you simply use `INSERT` or `APPEND`, running a pipeline twice will double your data.
By using an `UPSERT` (via Delta Lake's `MERGE INTO`), you can run the pipeline 1,000 times and the final state of the table will always be exactly identical, without a single duplicate.

## How Delta Lake Handles It
Under the hood, Data Lakes are just Parquet files on cloud storage. Parquet files are immutable (they cannot be edited). 
Delta Lake solves this by tracking files in a `_delta_log` JSON directory. When you run an UPSERT, Delta Lake secretly rewrites the Parquet file containing the updated row, and updates the transaction log to point to the new file, ignoring the old one.
