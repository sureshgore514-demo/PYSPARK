'''
Q. Your spark job is slow because one key has millions of records. How do you fix it.

Solution:-
    Identify skewed key
    Use salting
    Increase shuffle partitions size

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, rand

spark = SparkSession.builder \
    .appName("Late data arriving example") \
    .getOrCreate()

df = spark.read.parquet("gs://my-bucket/raw_data.parquet")

df_salted = df.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand()*10).cast("int"))
)

df_salted.groupBy("salted_key").count()
