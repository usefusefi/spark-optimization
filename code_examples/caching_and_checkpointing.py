from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("CachingAndCheckpointing") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

df = spark.read.parquet("large_data.parquet")

# Cache frequently used DataFrame
df.persist(StorageLevel.MEMORY_AND_DISK)

# Perform heavy computation
result_df = df.groupBy("category").count()

# Checkpoint to avoid recomputation
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
result_df.checkpoint()

result_df.show()
