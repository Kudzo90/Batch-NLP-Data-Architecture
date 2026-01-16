from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, collect_list, quarter, year

def create_spark_session():
    return SparkSession.builder \
        .appName("BabelBriefings-QuarterlyAgg") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def main(quarter_date):
    spark = create_spark_session()
    
    # Determine quarter months
    yr = quarter_date[:4]
    month = int(quarter_date[5:7])
    q = (month - 1) // 3 + 1
    
    # Read all months in quarter
    months = [(q-1)*3 + i for i in range(1, 4)]
    dfs = []
    for m in months:
        path = f"/data/cleaned/year={yr}/month={m:02d}/"
        try:
            dfs.append(spark.read.parquet(path))
        except:
            pass
    
    if not dfs:
        return
    
    from functools import reduce
    df = reduce(lambda a, b: a.union(b), dfs)
    
    # Aggregate by topic/language
    # Rename hyphenated columns for easier access
    df2 = df.withColumnRenamed("source-name", "source_name") \
            .withColumnRenamed("source-id", "source_id")

    # Aggregate by language and source
    agg_df = df2.groupBy("language", "source_name").agg(
        count("*").alias("article_count"),
        avg("sentiment_score").alias("avg_sentiment")
    )
        
    # Write to curated zone
    curated_path = f"/data/curated/year={yr}/quarter={q}/"
    agg_df.write.mode("overwrite").parquet(curated_path)
    
    # # Also write to ClickHouse
    # agg_df.write \
    #     .format("jdbc") \
    #     .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
    #     .option("dbtable", f"quarterly_nlp_q{q}_{yr}") \
    #     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    #     .mode("overwrite") \
    #     .save()
    
    # spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--quarter", required=True)
    args = parser.parse_args()
    main(args.quarter)