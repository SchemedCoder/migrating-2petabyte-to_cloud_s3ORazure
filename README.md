# Universal 2PB Cloud Data Migration Learning Repository

This repository serves as a **Learning Template** for PySpark Data Engineers tasked with migrating Petabyte-scale data from on-premise distributed file systems (HDFS, NFS) directly into Cloud Object Storage.

Unlike standard JDBC extraction which pulls from relational databases row-by-row, this template handles massive file-to-file transfers, utilizing highly concurrent network streams, dynamic partition estimation, and cloud-specific Hadoop tuning parameters.

**It natively supports both AWS S3 (`s3a://`) and Azure Data Lake Storage Gen2 (`abfss://`).**

## Features
- **Dual Cloud Support:** Automatically detects your `target_uri` and applies the correct Hadoop Configurations for either AWS S3 or Azure ADLS.
- **Dynamic Partitioning:** Estimates the optimal number of parallel write streams based on `total_bytes` and `target_file_size` (e.g. 256MB).
- **Azure Authentication:** Supports both Azure Managed Identity and Service Principal (OAuth) credentials.
- **High Throughput Tuning:** Injects specific parameters like `fs.s3a.multipart.size` and `fs.azure.max.concurrent.requests` directly into the Spark Context.

## Usage: AWS S3 Target
```bash
spark-submit \
    --conf spark.executor.memory=32g \
    --conf spark.executor.cores=8 \
    migrate.py \
    --source-type parquet \
    --source-path hdfs://nn:8020/path/to/data \
    --target-uri s3a://my-bucket/path \
    --total-bytes 2199023255552 \
    --target-file-size 268435456
```

## Usage: Azure ADLS Gen2 Target (ABFS)
```bash
# Example using Managed Identity
spark-submit \
    --conf spark.executor.memory=32g \
    --conf spark.executor.cores=8 \
    migrate.py \
    --source-type parquet \
    --source-path hdfs://nn:8020/path/to/data \
    --target-uri abfss://container@account.dfs.core.windows.net/prefix \
    --total-bytes 2199023255552 \
    --target-file-size 268435456 \
    --use-managed-identity true
```

*Note: If using an Azure Service Principal instead of Managed Identity, ensure you pass the variables via environment variables `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID`, or pass them explicitly via the CLI arguments.*
