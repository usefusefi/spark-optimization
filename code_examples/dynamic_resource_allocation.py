from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DynamicResourceAllocation") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .getOrCreate()

df = spark.read.parquet("large_dataset.parquet")

df.groupBy("category").count().show()
