# Migration Scenarios Deep-Dive (How to Execute the Matrix)

This document breaks down the specific steps and prerequisites required to execute the migration methods outlined in the `01_Ingestion_Patterns_Matrix.md`. 

## 1. Relational DB -> Cloud Data Warehouse (e.g., PostgreSQL to Snowflake)
**Method Used:** Log-Based Change Data Capture (CDC) via Fivetran or Airbyte.
**How to actually execute this:**
1. **Source Configuration:** You cannot just connect a tool to a database and expect real-time syncs. You must alter the database configuration. For PostgreSQL, you must set `wal_level = logical` in the `postgresql.conf` file. This tells Postgres to keep a detailed log of every INSERT, UPDATE, and DELETE.
2. **Tool Setup:** Create a dedicated database user (e.g., `fivetran_user`) with `REPLICATION` privileges.
3. **Target Setup:** In Snowflake, grant the tool permissions to create schemas and tables. The tool will read the Write-Ahead Logs (WAL) and automatically UPSERT the changes into Snowflake.

## 2. On-Prem HDFS -> Cloud Object Storage (e.g., Hadoop to ADLS Gen2)
**Method Used:** Distributed PySpark / AzCopy / Azure Data Box.
**How to actually execute this:**
1. **Assessment:** If the data is > 100 Terabytes and you only have a 1 Gbps network, you must order an **Azure Data Box**. 
2. **Network Method:** If using network transfer, you must provision an Azure ExpressRoute (dedicated private fiber connection). 
3. **Execution:** Use the `migrate.py` script provided in this repository. Ensure your Spark cluster has enough worker nodes so that the aggregate network bandwidth of the cluster matches your ExpressRoute bandwidth (e.g., 50 nodes x 2 Gbps = 100 Gbps total throughput).

## 3. Relational DB -> Cloud Relational DB (e.g., On-Prem SQL Server to Azure SQL)
**Method Used:** Azure Database Migration Service (DMS) / Log Shipping.
**How to actually execute this:**
1. **Pre-Migration:** Run the **Data Migration Assistant (DMA)** tool from Microsoft to check for compatibility issues (e.g., unsupported legacy stored procedures).
2. **Offline vs Online:** 
   - *Offline:* Backup the database to a `.bak` file, upload to Azure Blob, and restore it on Azure SQL.
   - *Online:* Setup Azure DMS. It will perform a full initial load, and then continuously sync transaction logs until you are ready to cut over (flip the DNS switch) with zero downtime.

## 4. SaaS API -> Cloud Data Warehouse (e.g., Zendesk to Snowflake)
**Method Used:** Python REST API Scripts or ELT Tools.
**How to actually execute this:**
1. **Authentication:** Obtain OAuth 2.0 credentials or API Bearer Tokens. Store them in **Azure Key Vault** (NEVER in code).
2. **Pipeline Logic:** Use the `api_pagination_backoff.py` script. You must write logic to save the `last_modified_date` from the API response into a control table. The next time the pipeline runs, it queries the API appending `?updated_after={last_modified_date}` so you only pull new records.
