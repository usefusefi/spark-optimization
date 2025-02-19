from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PartitioningOptimization") \
    .getOrCreate()

df = spark.read.parquet("large_dataset.parquet")

# Repartition for better parallelism
df = df.repartition(100)  # Adjust based on cluster resources

# Coalesce to minimize shuffle overhead after heavy transformations
df = df.coalesce(10)

df.write.mode("overwrite").parquet("optimized_output.parquet")

print("Partitioning optimization applied successfully!")
