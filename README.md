# Enterprise Data Migration & Engineering Repository

This repository is a comprehensive learning and implementation guide for Enterprise Data Engineering. It covers the full spectrum of data ingestion, from transferring 2 Petabytes of raw files, to pulling 300 million rows from an RDBMS, to handling rate-limited REST APIs and building Delta Lake CDC pipelines.

## 📚 Theory Directory (Architectural Concepts)
Before deploying code, engineers must understand the network and architectural boundaries.
* [`theory/01_Ingestion_Patterns_Matrix.md`](theory/01_Ingestion_Patterns_Matrix.md) - Permutations and combinations of Source-to-Target migrations.
* [`theory/02_Network_and_Security_Boundaries.md`](theory/02_Network_and_Security_Boundaries.md) - Details on Public, Private/Hybrid (SHIR), Managed VNet, and Air-Gapped architectures.
* [`theory/03_Medallion_Architecture.md`](theory/03_Medallion_Architecture.md) - The Bronze, Silver, and Gold data lakehouse layers.

## 💻 Code Directory (Production-Ready Templates)
* [`migrate.py`](migrate.py) - (Root) The massive 2PB File-to-Cloud Object Storage migration script (AWS S3 & Azure ABFS).
* [`code/02_rdbms_jdbc_partitioning/jdbc_parallel_extract.py`](code/02_rdbms_jdbc_partitioning/jdbc_parallel_extract.py) - How to safely extract 30 Crore (300M) rows from SQL databases without locking the source.
* [`code/03_rest_api_ingestion/api_pagination_backoff.py`](code/03_rest_api_ingestion/api_pagination_backoff.py) - Robust REST API ingestion handling HTTP 429 Rate Limits and Cursor Pagination.
* [`code/04_cdc_delta_lake/merge_upsert_silver.py`](code/04_cdc_delta_lake/merge_upsert_silver.py) - Idempotent Change Data Capture (CDC) processing using Delta Lake `MERGE INTO`.
