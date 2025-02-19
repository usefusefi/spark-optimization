from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("JoinsOptimization").getOrCreate()

df_large = spark.read.parquet("large_data.parquet")
df_small = spark.read.parquet("small_lookup.parquet")

optimized_join = df_large.join(broadcast(df_small), "id", "inner")
optimized_join.show()
