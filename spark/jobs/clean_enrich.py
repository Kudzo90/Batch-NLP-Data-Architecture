from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, coalesce
from pyspark.sql.types import StringType, FloatType
import argparse
import re

def create_spark_session():
    return SparkSession.builder \
        .appName("BabelBriefings-CleanEnrich") \
        .getOrCreate()

def clean_text(text):
    if text is None:
        return ""
    # Remove special characters and normalize whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text

def get_sentiment(text):
    # Placeholder sentiment score: 0.0 for now
    return 0.0

def main(month):
    spark = create_spark_session()
    
    year = month[:4]
    mon = month[5:7]
    
    raw_path = f"/data/raw_partitioned/year={year}/month={mon}"
    cleaned_path = f"/data/cleaned/year={year}/month={mon}"
    
    df = spark.read.parquet(raw_path)
    
    clean_udf = udf(clean_text, StringType())
    sentiment_udf = udf(get_sentiment, FloatType())
    
    enriched_df = df.withColumn(
    "clean_headline",
    clean_udf(coalesce(col("en-title"), col("title")))
).withColumn(
    "sentiment_score",
    sentiment_udf(col("clean_headline"))
).filter(col("clean_headline") != "")
    
    enriched_df.write.mode("overwrite").parquet(cleaned_path)
    
    print(f"Cleaned and enriched data written to {cleaned_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()
    main(args.month)