'''
compare current salary with previous employee salary and calculate difference between employee salary
and previous employee salary.

+------+----+------+-----+-----------+----+
|   100|   A|  1000|   IT|       NULL|NULL|
|   101|   B|  1500|   IT|       1000| 500|
|   102|   C|  2000|   IT|       1500| 500|
|   103|   O|  2700|Sales|       NULL|NULL|
|   104|   N|  5400|Sales|       2700|2700|
|   105|   M|  5400|Sales|       5400|   0|
+------+----+------+-----+-----------+----+

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

window = Window.partitionBy("dept").orderBy("salary")

df2 = df.withColumn("prev_salary", lag("salary").over(window)) \
        .withColumn("diff", col("salary") - col("prev_salary"))

df2.show()

spark.stop()