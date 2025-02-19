from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MemoryTuning") \
    .config("spark.memory.fraction", "0.75") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

df = spark.read.parquet("data.parquet")
df.cache()

print("Dataframe Cached")
