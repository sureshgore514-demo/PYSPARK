'''
calculate student marks per percentage

output:
+---+------+----------+-----------+
| Id|  Name|Percentage|      Grade|
+---+------+----------+-----------+
|100| Parth|      85.0|Distinction|
|101|Swaraj|      88.0|Distinction|
|102| Sanvi|      92.0|Distinction|
|104| Tanvi|      92.5|Distinction|
+---+------+----------+-----------+

'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Calculate student marks") \
    .getOrCreate()

data1 = [(100,"Parth"),
         (101,"Swaraj"),
         (102,"Sanvi"),
         (104,"Tanvi")]

data2 = [(100,"Pyspark",80),
         (101,"Pyspark",86),
         (100,"Python",90),
         (101,"Python",90),
         (102,"Java",90),
         (102,"C++",94),
         (104,"Science",88),
         (104,"Maths",97)]

schema1 = ["Id","Name"]
schema2 = ["Id","Subject","Mark"]

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)

#df1.show()
#df2.show()

df_join = df1.join(df2, df1.Id == df2.Id, how="inner").drop(df2.Id)

#df_join.show()

df_percentage = df_join.groupBy(["Id","Name"]).agg((sum("Mark")/count("*")).alias("Percentage"))

df_result = df_percentage.select('*',
                     (when(df_percentage.Percentage >= 70 , 'Distinction')
                     .when((df_percentage.Percentage < 70) & (df_percentage.Percentage >=60), 'First Class')
                     .when((df_percentage.Percentage < 60) & (df_percentage.Percentage > 50), 'Second Class')
                     .when((df_percentage.Percentage < 50) & (df_percentage.Percentage >= 40), 'Third Class')
                     .when((df_percentage.Percentage < 40),'Fail')
                     ).alias("Grade"))
df_result.show()

#df_percentage.show()

spark.stop()