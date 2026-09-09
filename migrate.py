#!/usr/bin/env python3
"""
migrate.py
PySpark batch migration learning template for large-scale data transfer (2PB+) 
to both AWS S3 (s3a://) and Azure ADLS Gen2 (abfss://).

Usage (Azure ABFS example):
  spark-submit migrate.py \
      --source-type parquet \
      --source-path hdfs://nn:8020/path/to/data \
      --target-uri abfss://container@account.dfs.core.windows.net/prefix \
      --total-bytes 2199023255552 \
      --use-managed-identity true

Usage (AWS S3 example):
  spark-submit migrate.py \
      --source-type parquet \
      --source-path hdfs://nn:8020/path/to/data \
      --target-uri s3a://my-bucket/prefix \
      --total-bytes 2199023255552
"""
import argparse
import logging
import math
import os
import re
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

LOG = logging.getLogger("migrate")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def build_spark(app_name="universal-migrate-2pb", extra_conf=None):
    builder = SparkSession.builder.appName(app_name)
    # Generic tuning placeholders - customize per cluster instance/VM SKU
    builder = builder.config("spark.sql.shuffle.partitions", "2000") \
                     .config("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024)) \
                     .config("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    return spark

def set_hadoop_conf(spark, conf_dict):
    """
    Set Hadoop configuration entries (via Java HadoopConfiguration) for S3A or ABFS tuning.
    """
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    for k, v in conf_dict.items():
        hconf.set(k, v)

# ==========================================
# AWS S3 Configuration
# ==========================================
def configure_s3(spark, s3_endpoint=None):
    LOG.info("Configuring Hadoop properties for AWS S3 (s3a://)...")
    s3_opts = {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.multipart.size": str(64 * 1024 * 1024), # 64MB parts
        "fs.s3a.threads.max": "512",
        "fs.s3a.fast.upload": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.buffer.dir": "/mnt/spark_s3_buffer",
    }
    if s3_endpoint:
        s3_opts["fs.s3a.endpoint"] = s3_endpoint
    set_hadoop_conf(spark, s3_opts)

# ==========================================
# Azure ADLS Gen2 (ABFS) Configuration
# ==========================================
def parse_abfss_target(abfss_uri):
    # Strip scheme: abfss://container@account.dfs.core.windows.net/prefix
    m = re.match(r"abfss://([^@]+)@([^/]+)(?:/(.*))?", abfss_uri)
    if not m:
        raise ValueError("Invalid abfss URI: {}".format(abfss_uri))
    container = m.group(1)
    host = m.group(2)  # account.dfs.core.windows.net
    prefix = m.group(3) or ""
    account = host.split(".")[0]
    return account, container, prefix

def configure_abfs_for_account(spark, account, use_managed_identity=False,
                               client_id=None, client_secret=None, tenant_id=None,
                               abfs_opts_override=None):
    LOG.info("Configuring Hadoop properties for Azure ADLS Gen2 (abfss://)...")
    conf = {}
    account_fqdn = f"{account}.dfs.core.windows.net"

    conf[f"fs.azure.account.auth.type.{account_fqdn}"] = "OAuth"

    if use_managed_identity:
        LOG.info("Using Azure Managed Identity for Auth.")
        conf[f"fs.azure.account.oauth.provider.type.{account_fqdn}"] = \
            "org.apache.hadoop.fs.azurebfs.oauth2.ManagedIdentityTokenProvider"
        if client_id:
            conf[f"fs.azure.account.oauth2.client.id.{account_fqdn}"] = client_id
    else:
        LOG.info("Using Azure Service Principal (Client Credentials) for Auth.")
        conf[f"fs.azure.account.oauth.provider.type.{account_fqdn}"] = \
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        if not (client_id and client_secret and tenant_id):
            LOG.warning("Service principal auth selected but AZURE_CLIENT_ID/SECRET/TENANT not provided fully!")
        if client_id:
            conf[f"fs.azure.account.oauth2.client.id.{account_fqdn}"] = client_id
        if client_secret:
            conf[f"fs.azure.account.oauth2.client.secret.{account_fqdn}"] = client_secret
        if tenant_id:
            conf[f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}"] = \
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    # Tuning knobs for high throughput
    conf[f"fs.azure.max.concurrent.requests"] = "256"
    conf[f"dfs.client.read.shortcircuit"] = "false"
    
    if abfs_opts_override:
        conf.update(abfs_opts_override)
        
    set_hadoop_conf(spark, conf)
    return conf

# ==========================================
# Core Migration Logic
# ==========================================
def estimate_partitions(total_bytes, target_file_size, parallelism_hint=None):
    files = max(1, int(math.ceil(total_bytes / float(target_file_size))))
    partitions = max(files, 1)
    if parallelism_hint:
        partitions = max(partitions, parallelism_hint)
    return partitions

def read_source(spark, source_type, source_path, read_opts):
    fmt = source_type.lower()
    return spark.read.format(fmt).options(**read_opts).load(source_path)

def write_target(df, target_uri, fmt="parquet", partition_cols=None, compression="zstd",
                 max_records_per_file=None, target_file_size=None):
    writer = df.write.mode("append").format(fmt)
    writer = writer.option("compression", compression)
    if max_records_per_file:
        writer = writer.option("maxRecordsPerFile", int(max_records_per_file))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_uri)

def main():
    parser = argparse.ArgumentParser()
    # Common Args
    parser.add_argument("--source-type", required=True, help="parquet|orc|csv|avro|...")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--target-uri", required=True, help="s3a://... or abfss://...")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--total-bytes", type=float, required=False)
    parser.add_argument("--target-file-size", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--parallelism", type=int, default=None)
    parser.add_argument("--partition-cols", default=None)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--max-records-per-file", type=int, default=None)
    
    # S3 Specific Args
    parser.add_argument("--s3-endpoint", default=None)
    
    # Azure Specific Args
    parser.add_argument("--use-managed-identity", type=lambda s: s.lower() == "true", default=False)
    parser.add_argument("--azure-client-id", default=None)
    parser.add_argument("--azure-client-secret", default=None)
    parser.add_argument("--azure-tenant-id", default=None)
    args = parser.parse_args()

    # Pre-Spark Configuration overrides
    extra_conf = {}
    if args.target_uri.startswith("abfss://"):
        extra_conf.update({
            "spark.hadoop.fs.abfss.impl": "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            "spark.sql.files.openCostInBytes": str(4 * 1024 * 1024),
        })

    spark = build_spark(extra_conf=extra_conf)
    LOG.info("Spark started: %s", spark)

    # Apply Storage specific tuning via Hadoop configs
    if args.target_uri.startswith("s3a://"):
        configure_s3(spark, args.s3_endpoint)
    elif args.target_uri.startswith("abfss://"):
        account, container, prefix = parse_abfss_target(args.target_uri)
        client_id = args.azure_client_id or os.environ.get("AZURE_CLIENT_ID")
        client_secret = args.azure_client_secret or os.environ.get("AZURE_CLIENT_SECRET")
        tenant_id = args.azure_tenant_id or os.environ.get("AZURE_TENANT_ID")
        configure_abfs_for_account(spark, account, args.use_managed_identity, client_id, client_secret, tenant_id)
    else:
        LOG.warning("Target URI does not start with s3a:// or abfss://. Bypassing specific tuning.")

    # Read
    read_opts = {"header": "true", "inferSchema": "false"} if args.source_type.lower() == "csv" else {}
    df = read_source(spark, args.source_type, args.source_path, read_opts)

    # Estimate Partitions
    partitions = estimate_partitions(args.total_bytes, args.target_file_size, args.parallelism) if args.total_bytes else None
    if partitions:
        if args.partition_cols:
            pcols = [c.strip() for c in args.partition_cols.split(",") if c.strip()]
            df = df.repartition(partitions, *[F.col(c) for c in pcols])
        else:
            df = df.repartition(partitions)
        LOG.info("Repartitioned to %d partitions", partitions)

    # Write
    partition_cols = [c.strip() for c in args.partition_cols.split(",")] if args.partition_cols else None
    start = time.time()
    try:
        write_target(df, args.target_uri, fmt=args.format, partition_cols=partition_cols,
                     compression=args.compression, max_records_per_file=args.max_records_per_file)
    except Exception as e:
        LOG.exception("Write failed: %s", e)
        raise
    finally:
        duration = time.time() - start
        LOG.info("Write finished in %.2f seconds", duration)
        spark.stop()

if __name__ == "__main__":
    main()
