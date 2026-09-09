#!/usr/bin/env python3
"""
migrate.py
PySpark batch migration template for large-scale data transfer to S3 / Azure Blob / staging for Snowflake.

Usage:
  spark-submit \
    --conf spark.executor.memory=... \
    --conf spark.executor.cores=... \
    migrate.py \
    --source-type parquet \
    --source-path hdfs://nn:8020/path/to/data \
    --target-uri s3a://my-bucket/path \
    --total-bytes 2199023255552 \
    --target-file-size 268435456 \
    --parallelism 4096 \
    --format parquet \
    --partition-cols dt,region
"""
import argparse
import logging
import sys
import math
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

LOG = logging.getLogger("migrate")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def build_spark(app_name="large-migrate", extra_conf=None):
    builder = SparkSession.builder.appName(app_name)
    
    # Generic tuning placeholders - customize per cloud and instance types
    builder = builder.config("spark.sql.shuffle.partitions", "2000") \
                     .config("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024)) \
                     .config("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    return spark

def set_s3_options(spark, s3_opts):
    # Example Hadoop/S3A tuning for high parallelism uploads
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    for k, v in s3_opts.items():
        hconf.set(k, v)

def estimate_partitions(total_bytes, target_file_size, parallelism_hint=None):
    # number of output files = total_bytes / target_file_size
    files = max(1, int(math.ceil(total_bytes / float(target_file_size))))
    # partitions (Spark tasks) should be >= files and also tuned for cluster
    partitions = max(files, 1)
    if parallelism_hint:
        partitions = max(partitions, parallelism_hint)
    # cap partitions to avoid tiny tasks (caller should choose reasonable parallelism)
    return partitions

def read_source(spark, source_type, source_path, read_opts):
    fmt = source_type.lower()
    if fmt in ("parquet", "orc", "avro"):
        df = spark.read.format(fmt).options(**read_opts).load(source_path)
    elif fmt in ("csv", "text"):
        df = spark.read.format(fmt).options(**read_opts).load(source_path)
    else:
        # generic format fallback
        df = spark.read.format(fmt).options(**read_opts).load(source_path)
    return df

def write_target(df, target_uri, fmt="parquet", partition_cols=None, compression="zstd",
                 max_records_per_file=None, target_file_size=None):
    writer = df.write.mode("append").format(fmt)
    writer = writer.option("compression", compression)
    if max_records_per_file:
        writer = writer.option("maxRecordsPerFile", int(max_records_per_file))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_uri)

def stage_for_snowflake(s3_uri_prefix, snowflake_stage_name, aws_role_arn=None):
    # Placeholder: write files to S3 prefix, create Snowflake stage, then COPY INTO.
    pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-type", required=True, help="parquet|orc|csv|avro|...")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--target-uri", required=True, help="s3a://bucket/prefix or abfss://container@acct.dfs.core.windows.net/prefix")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--total-bytes", type=float, required=False,
                        help="Estimated total bytes to migrate (used to compute partitions). Optional but recommended.")
    parser.add_argument("--target-file-size", type=int, default=256 * 1024 * 1024,
                        help="Target output file size in bytes (default 256MB).")
    parser.add_argument("--parallelism", type=int, default=None, help="Minimum desired parallelism (num partitions).")
    parser.add_argument("--partition-cols", default=None, help="Comma-separated partition columns for target.")
    parser.add_argument("--compression", default="zstd", help="parquet compression codec (zstd/snappy/gzip)")
    parser.add_argument("--max-records-per-file", type=int, default=None)
    parser.add_argument("--s3-endpoint", default=None)
    args = parser.parse_args()

    extra_conf = {}
    # Example S3 tuning: tune for high parallel upload concurrency
    if args.target_uri.startswith("s3a://"):
        extra_conf.update({
            "spark.hadoop.fs.s3a.connection.maximum": "1000",
            "spark.hadoop.fs.s3a.threads.max": "512",
            "spark.hadoop.fs.s3a.multipart.size": str(64 * 1024 * 1024),  # 64MB parts
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })
        if args.s3_endpoint:
            extra_conf["spark.hadoop.fs.s3a.endpoint"] = args.s3_endpoint

    spark = build_spark(extra_conf=extra_conf)
    LOG.info("Spark started: %s", spark)

    # Additional S3 Hadoop configuration via sparkContext
    s3_opts = {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.multipart.size": str(64 * 1024 * 1024),
        "fs.s3a.threads.max": "512",
        "fs.s3a.buffer.dir": "/mnt/spark_s3_buffer",
        # Ensure credentials set via environment / instance role
    }
    if args.target_uri.startswith("s3a://"):
        set_s3_options(spark, s3_opts)

    read_opts = {}
    if args.source_type.lower() == "csv":
        read_opts.update({"header": "true", "inferSchema": "false"})
    df = read_source(spark, args.source_type, args.source_path, read_opts)
    LOG.info("Schema read: %s", df.schema.simpleString())

    # Estimate partitions
    partitions = None
    if args.total_bytes:
        partitions = estimate_partitions(args.total_bytes, args.target_file_size, args.parallelism)
        LOG.info("Estimated partitions based on total_bytes=%s, target_file_size=%s -> %s partitions",
                 args.total_bytes, args.target_file_size, partitions)

    if partitions:
        # Repartition by hash of partition columns if provided, else by round-robin
        if args.partition_cols:
            pcols = [c.strip() for c in args.partition_cols.split(",") if c.strip()]
            df = df.repartition(partitions, *[F.col(c) for c in pcols])
        else:
            df = df.repartition(partitions)
        LOG.info("Repartitioned to %d partitions", partitions)
    else:
        LOG.info("No partitions computed; using source partitions")

    partition_cols = [c.strip() for c in args.partition_cols.split(",")] if args.partition_cols else None

    start = time.time()
    try:
        write_target(df,
                     args.target_uri,
                     fmt=args.format,
                     partition_cols=partition_cols,
                     compression=args.compression,
                     max_records_per_file=args.max_records_per_file,
                     target_file_size=args.target_file_size)
    except Exception as e:
        LOG.exception("Write failed: %s", e)
        raise
    finally:
        duration = time.time() - start
        LOG.info("Write finished in %.2f seconds", duration)
        spark.stop()

if __name__ == "__main__":
    main()
