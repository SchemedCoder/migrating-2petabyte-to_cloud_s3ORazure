# The Industry Standard Ingestion Matrix

When moving data at an enterprise scale, the tools you use depend entirely on the *permutation* of your Source and Target.

| Source System | Target System | Real-World Example | Industry Standard Tools & Methods |
| :--- | :--- | :--- | :--- |
| **Big Data Files / HDFS** *(On-Prem)* | **Cloud Object Storage** *(Data Lake)* | On-Prem Hadoop clusters -> AWS S3 or Azure ADLS Gen2 | **PySpark/Databricks**, AWS DataSync, Azure Data Box, AzCopy. |
| **Relational DB** *(On-Prem)* | **Relational DB** *(Cloud)* | On-Prem SQL Server -> Azure SQL DB | Azure DMS, AWS DMS, native Log Shipping. |
| **Relational DB** *(On-Prem/Cloud)* | **Cloud Data Warehouse** | PostgreSQL -> Snowflake, Redshift, or BigQuery | Fivetran, Airbyte, Azure Data Factory (ADF), dbt. |
| **Cloud Object Storage** | **Cloud Data Warehouse** | AWS S3 or Azure ADLS -> Snowflake | **Bulk Copy Commands:** Snowflake `COPY INTO`, Databricks Auto Loader. |
| **NoSQL Database** | **Cloud NoSQL Database** | MongoDB -> Azure CosmosDB | Kafka Connect, native replication. |
| **SaaS Applications** | **Cloud Data Warehouse** | Salesforce / REST APIs -> Snowflake | ELT Platforms (Fivetran/Airbyte) or custom Python pagination scripts. |
