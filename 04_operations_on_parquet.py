from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SalesAggregation") \
    .getOrCreate()

df = spark.read.parquet("C:\DEVELOPMENT\PYSPARK\data/Titanic Passengers.parquet")

res = df.select("PassengerId","Name","Sex")

print(res)

# print(df.collect())
spark.stop()
