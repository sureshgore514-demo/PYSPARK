'''
In pyspark the explode() is used to convert array or map columns into multiple rows.
ex [a,b,c]
==> a
    b
    c

What does explode() do?
==> Take one row with an array or map
Create one row per element
Duplicate the values of other columns

'''

# Explode an array column values

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# create session object.
spark = SparkSession.builder.getOrCreate()

data = [
    (100,["Spark","Java","Python"]),
    (101,["GCP","Azure","AWS"])
]

# create dataframe.
df = spark.createDataFrame(data,["ID","Skills"])
print("Before Explode()")
df.show()

df_exploded = df.select("ID",explode("Skills").alias("Skill"))
print("After explode()")
df_exploded.show()


# explode a Map column

data1 = [
    (1, {"Math": 90, "Science": 95}),
    (2, {"English": 85})
]

df1 = spark.createDataFrame(data1, ["id", "marks"])

df1.select("id", explode("marks").alias("subject", "score")).show()

spark.stop()
