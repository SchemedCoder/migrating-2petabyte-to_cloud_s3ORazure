# Enterprise Data Migration & Engineering Repository

This repository is a comprehensive learning and implementation guide for Enterprise Data Engineering. It covers the full spectrum of data ingestion, from transferring 2 Petabytes of raw files, to pulling 300 million rows from an RDBMS, handling rate-limited REST APIs, and building Delta Lake CDC/UPSERT pipelines.

## 📚 Theory Directory (Architectural Concepts)
Before deploying code, engineers must understand the network and architectural boundaries.
* [`theory/01_Ingestion_Patterns_Matrix.md`](theory/01_Ingestion_Patterns_Matrix.md) - Permutations and combinations of Source-to-Target migrations.
* [`theory/02_Network_and_Security_Boundaries.md`](theory/02_Network_and_Security_Boundaries.md) - Details on Public, Private/Hybrid (SHIR), Managed VNet, and Air-Gapped architectures.
* [`theory/03_Medallion_Architecture.md`](theory/03_Medallion_Architecture.md) - The Bronze, Silver, and Gold data lakehouse layers.
* [`theory/04_Migration_Scenarios_DeepDive.md`](theory/04_Migration_Scenarios_DeepDive.md) - **[NEW]** Step-by-step requirements for executing the industry matrix permutations.
* [`theory/05_Upsert_and_CDC_Theory.md`](theory/05_Upsert_and_CDC_Theory.md) - **[NEW]** The theory behind UPSERTs, Change Data Capture, and Idempotency.

## 💻 Code Directory (Production-Ready Templates)
* [`migrate.py`](migrate.py) - (Root) The massive 2PB File-to-Cloud Object Storage migration script (AWS S3 & Azure ABFS).
* [`code/02_rdbms_jdbc_partitioning/jdbc_parallel_extract.py`](code/02_rdbms_jdbc_partitioning/jdbc_parallel_extract.py) - Extracting 300M rows from SQL databases without locking.
* [`code/03_rest_api_ingestion/api_pagination_backoff.py`](code/03_rest_api_ingestion/api_pagination_backoff.py) - REST API ingestion handling HTTP 429 Rate Limits.
* [`code/04_cdc_delta_lake/merge_upsert_silver.py`](code/04_cdc_delta_lake/merge_upsert_silver.py) - Idempotent Change Data Capture (CDC) processing using Delta Lake `MERGE INTO`.
* [`code/05_upsert_scenarios/scd_type_1_upsert.py`](code/05_upsert_scenarios/scd_type_1_upsert.py) - **[NEW]** Fully runnable PySpark code simulating an SCD Type 1 UPSERT.
