'''
coalsce() function will retrun first not null value from the list of columns or expression.
- It works row by row

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col

spark = SparkSession.builder.appName("coalsce").getOrCreate()

data = [
    (100,None,"Pune"),
    (200,"Mumbai", None),
    (300,None,"Nashik")
]

df = spark.createDataFrame(data,["ID","City1","City2"])
print("Before coalsce:")
df.show()

print("After coalsce:")
df.withColumn(
    "Final_value",
    coalesce(col("City1"),col("City2"))).show()

spark.stop()

