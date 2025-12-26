
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Cricket Match Problems").getOrCreate()

# Sample data
data = [
    ("India", "Australia", "India"),
    ("India", "New Zealand", "India"),
    ("India", "Srilanka", "India"),
    ("England", "Pakistan", "England"),
    ("South Africa", "New Zealand", "New Zealand"),
    ("India", "England", "India"),
    ("Australia", "Pakistan", "Australia"),
    ("Australia", "England", "Australia"),
    ("India", "Pakistan", "India"),
    ("South Africa", "New Zealand", "South Africa"),
    ("South Africa", "Srilanka", "South Africa"),
    ("Pakistan", "New Zealand", "New Zealand")
]

# Column names
columns = ["Team1", "Team2", "Winner"]

# create dataframe
df = spark.createDataFrame(data, schema=columns)
#df.show()

# Step 1: Set the win_flag here 1 or 0
df2 = df.select(F.col("Team1").alias("Team"),"Winner") \
    .union(df.select(F.col("Team2").alias("Team"),"Winner")) \
    .withColumn('win_flag',when(col("Team") == col('Winner'), 1).otherwise(0))
# df2.show()

# Step 2: Calculate the point table
df2.groupBy("Team").agg(F.count("*").alias("Total No Of Matches Played"),
                        F.sum("win_flag").alias("Total No Of Matches Won")) \
                        .withColumn("Points", F.col("Total No Of Matches Won") * 2) \
                        .sort(F.desc("Total No Of Matches Won"),F.desc("Total No Of Matches Played")) \
                        .show()

# Find the matches won by each team.
# df.groupBy("Winner").count().show()

spark.stop()