from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_timestamp
import argparse
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("BabelBriefings-Ingest") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    target_year = args.month[:4]
    target_mon = args.month[5:7]
    
    # Read CSV
    df = spark.read.option("header", "true").csv(args.source)
    
    # Parse and filter
    df = df.withColumn("published_ts", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    monthly_df = df.filter(
        (year(col("published_ts")) == int(target_year)) &
        (month(col("published_ts")) == int(target_mon))
    )
    
    # Write locally
    output_path = f"/data/raw/year={target_year}/month={target_mon}"
    monthly_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Ingested {monthly_df.count()} records to {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()