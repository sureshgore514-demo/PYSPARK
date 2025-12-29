'''
Problem :- calculate dept wise highest salary

output:
Highest salary of emp from each department
+------+----+------+-----+----+
|emp_id|name|salary| dept|rank|
+------+----+------+-----+----+
|   102|   C|  2000|   IT|   1|
|   104|   N|  5400|Sales|   1|
|   105|   M|  5400|Sales|   1|
+------+----+------+-----+----+

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

#df.show()

window_spec = Window.partitionBy("dept").orderBy(desc("salary"))

result = df.withColumn(
    "rank",
    dense_rank().over(window_spec)
).filter('rank=1')

print('Highest salary of emp from each department')
result.show()

spark.stop()