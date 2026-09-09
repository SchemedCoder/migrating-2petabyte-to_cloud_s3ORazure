# Migrating 2 Petabytes

This repository contains a PySpark ETL template designed specifically for transferring massive volumes (up to 2PB) of file-based data (Parquet, CSV, Avro, ORC) from HDFS/NFS to Cloud Object Stores like S3, Azure Blob Storage (ABFS), or Snowflake staging areas.

## Usage
Submit via `spark-submit`:
```bash
spark-submit \
    --conf spark.executor.memory=32g \
    --conf spark.executor.cores=8 \
    migrate.py \
    --source-type parquet \
    --source-path hdfs://nn:8020/path/to/data \
    --target-uri s3a://my-bucket/path \
    --total-bytes 2199023255552 \
    --target-file-size 268435456 \
    --parallelism 4096 \
    --format parquet
```
