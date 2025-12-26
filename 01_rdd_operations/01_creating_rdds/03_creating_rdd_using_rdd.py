'''

author : suresh
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("creating rdd using rdd") \
    .getOrCreate()

data = [10,20,30,40,50]
old_rdd = spark.sparkContext.parallelize(data)

new_rdd = old_rdd.map(lambda x: x * x)
print(' Old RDD : ',old_rdd.collect(),'\n','New RDD : ',new_rdd.collect())

spark.stop()


