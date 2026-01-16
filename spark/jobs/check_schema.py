from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaCheck").getOrCreate()
df = spark.read.parquet("/data/cleaned/year=2020/month=08")
df.printSchema()
df.show(5, truncate=False)
spark.stop()