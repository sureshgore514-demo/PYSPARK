'''
calculate the running total of salary by department

output:
+------+----+------+-----+-------------+
|   100|   A|  1000|   IT|         1000|
|   101|   B|  1500|   IT|         2500|
|   102|   C|  2000|   IT|         4500|
|   103|   O|  2700|Sales|         2700|
|   104|   N|  5400|Sales|         8100|
|   105|   M|  5400|Sales|        13500|
+------+----+------+-----+-------------+

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName('Department wise highest salary') \
    .getOrCreate()

data = [
    (100,'A',1000,'IT'),
    (101,'B',1500,'IT'),
    (102,'C',2000,'IT'),
    (103,'O',2700,'Sales'),
    (104,'N',5400,'Sales'),
    (105,'M',5400,'Sales')
]

schema = ['emp_id','name','salary','dept']

df = spark.createDataFrame(data, schema)

window = Window.partitionBy("dept") \
               .orderBy("salary") \
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_total", sum("salary").over(window)) \
  .show()

spark.stop()