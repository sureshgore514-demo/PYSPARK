'''
calculate student marks per percentage

'''
from Tools.scripts.dutree import display
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

df_percentage.show()

spark.stop()

