#!/usr/bin/env python3
"""
Ingest Babel Briefings JSON files (all languages), extract timestamps,
and write partitioned Parquet files by year/month.

Usage examples:
# Local target (writes to /data/raw/year=YYYY/month=MM/)
spark-submit /opt/spark-jobs/ingest_all_languages.py \
  --source "/data/babel-briefings-v1-*.json" \
  --target "/data/raw" \
  --overwrite

# MinIO target (s3a) - ensure SparkSession is configured with s3a credentials
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/ingest_all_languages.py \
  --source "/data/babel-briefings-v1-*.json" \
  --target "s3a://datalake/raw" \
  --overwrite
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, coalesce, lit
)
from pyspark.sql.types import StringType

# Candidate timestamp field names (common)
TIMESTAMP_CANDIDATES = [
    "publishedAt", "published_at", "published", "pubDate", "publishedDate", "date", "timestamp", "collectedAt", "collected_at", "collected"
    
]

# Candidate formats to try parsing (Spark format strings)
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssXXX",   # timezone-aware if present
]

def create_spark(app_name="BabelBriefingsIngest", s3=False, s3_endpoint=None, access_key=None, secret_key=None):
    builder = SparkSession.builder.appName(app_name)
    spark = builder.getOrCreate()

    if s3:
        # minimal recommended s3a settings for MinIO
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint or "http://minio:9000")
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key or "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key or "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark

def find_timestamp_column(df):
    cols = [c.lower() for c in df.columns]
    for cand in TIMESTAMP_CANDIDATES:
        if cand.lower() in cols:
            # return column name in original case
            for c in df.columns:
                if c.lower() == cand.lower():
                    return c
    return None

def parse_timestamp_expr(colname):
    """Return expression that tries multiple formats via coalesce"""
    exprs = []
    for fmt in TIMESTAMP_FORMATS:
        exprs.append(to_timestamp(col(colname), fmt))
    # also try plain cast if it's already timestamp type or numeric epoch
    exprs.append(to_timestamp(col(colname)))  # let Spark try default parsing
    return coalesce(*exprs)

def partition_key(yyyy_mm):
    yyyy, mm = yyyy_mm.split("-")
    return int(yyyy), int(mm)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True,
                        help="Source path (can include wildcard) for JSON files, e.g. /data/babel-briefings-v1-*.json")
    parser.add_argument("--target", required=True,
                        help="Target base path to write partitioned parquet, e.g. /data/raw or s3a://datalake/raw")
    parser.add_argument("--start", required=False,
                        help="Start month inclusive YYYY-MM (optional)")
    parser.add_argument("--end", required=False,
                        help="End month inclusive YYYY-MM (optional)")
    parser.add_argument("--sample", type=int, required=False,
                        help="Sample N records (for quick testing)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite partitions (default is append for existing partitions)")
    parser.add_argument("--s3-endpoint", required=False, help="S3/MinIO endpoint (http://minio:9000)")
    parser.add_argument("--s3-access-key", required=False, help="S3 access key")
    parser.add_argument("--s3-secret-key", required=False, help="S3 secret key")

    args = parser.parse_args()

    target_is_s3 = args.target.lower().startswith("s3a://") or args.s3_endpoint
    spark = create_spark(app_name="BabelBriefings-Ingest-All", s3=target_is_s3,
                         s3_endpoint=args.s3_endpoint, access_key=args.s3_access_key, secret_key=args.s3_secret_key)

    print(f"Reading JSON files from: {args.source}")
    # read JSON (multiLine True in case records contain newlines)
    df = spark.read.option("multiLine", "true").json(args.source)

    print("Schema preview:")
    df.printSchema()

    # find timestamp column
    ts_col = find_timestamp_column(df)
    if not ts_col:
        print("ERROR: could not find a timestamp column. Columns available:", df.columns)
        raise SystemExit(1)
    print(f"Using timestamp column: '{ts_col}'")

    # optionally sample
    if args.sample:
        print(f"Sampling {args.sample} records for quick test")
        df = df.limit(args.sample)

    # create parsed timestamp column using multiple formats
    ts_expr = parse_timestamp_expr(ts_col)
    df2 = df.withColumn("published_ts", ts_expr)

    # Drop rows where parsing failed and log counts per month
    df2 = df2.filter(col("published_ts").isNotNull())

    # add year and month
    df2 = df2.withColumn("year", year(col("published_ts")).cast(StringType())) \
             .withColumn("month", month(col("published_ts")).cast(StringType()))

    # Optionally filter by start/end range
    if args.start or args.end:
        def mm_to_int(s):
            return int(s.split("-")[0]) * 100 + int(s.split("-")[1])
        start_val = mm_to_int(args.start) if args.start else None
        end_val = mm_to_int(args.end) if args.end else None

        from pyspark.sql.functions import concat, lpad
        df2 = df2.withColumn("ym_int", (col("year").cast("int") * 100 + col("month").cast("int")))
        if start_val:
            df2 = df2.filter(col("ym_int") >= lit(start_val))
        if end_val:
            df2 = df2.filter(col("ym_int") <= lit(end_val))
        df2 = df2.drop("ym_int")

    # show counts by year-month
    print("Counts by year-month:")
    counts = df2.groupBy("year", "month").count().orderBy("year", "month")
    counts.show(truncate=False)

    # iterate partitions and write per year-month directory (idempotent control)
    partitions = counts.collect()
    base_target = args.target.rstrip("/")

    for r in partitions:
        y = r["year"]
        m = int(r["month"])
        month_str = f"{m:02d}"
        partition_df = df2.filter((col("year") == y) & (col("month") == str(m)))
        out_path = f"{base_target}/year={y}/month={month_str}/"

        write_mode = "overwrite" if args.overwrite else "append"
        print(f"Writing partition {y}-{month_str} -> {out_path} (mode={write_mode})")

        # Writing to a single partition path: overwrite will completely replace that dir
        partition_df.write.mode(write_mode).parquet(out_path)

    print("Ingestion complete.")
    spark.stop()

if __name__ == "__main__":
    main()