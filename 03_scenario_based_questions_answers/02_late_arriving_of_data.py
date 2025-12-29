'''
LATE ARRIVING DATA IN DAILY BATCH PIPELINNE

Q. Your pyspark job runs daily, but some file arrive late in GCS. How will you handle this.

SOLUTION:
    Use partition overwrite
    Re-process last N days
    Maintain watermark / processed date

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Late data arriving example") \
    .getOrCreate()

df = spark.read.parquet("gs://my-bucket/raw_data.parquet")

# Re-Process last 3 days data
df_filtered = df.filter(col("ingestion_date") > '2025-12-26')

# write reprocessed data into new bucket in GCS
df_filtered.write \
    .mode("overwrite") \
    .parquet("gs://my-bucket/data.parquet")

spark.stop()

