from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

spark = SparkSession.builder.appName("SkewHandling").getOrCreate()

df_large = spark.read.parquet("large_skewed_data.parquet")
df_small = spark.read.parquet("small_lookup_data.parquet")

# Add salting to avoid skewed key distribution
df_large = df_large.withColumn("salted_key", col("key") + (rand() * 10).cast("int"))
df_small = df_small.withColumn("salted_key", col("key") + (rand() * 10).cast("int"))

# Join using salted keys
df_joined = df_large.join(df_small, "salted_key", "inner")

df_joined.show()
