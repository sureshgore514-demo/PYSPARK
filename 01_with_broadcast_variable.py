from pyspark.sql import SparkSession
import re


# Create Spark session
spark = SparkSession.builder \
    .appName("WordCountWithBroadcast") \
    .getOrCreate()

# Stop WARN logs
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

# Input data
rdd = sc.textFile("wordcnt.txt")

# Stop words (small lookup data)
stop_words = ["is", "the", "and", "to", "of"]

# Broadcast stop words
broadcast_stop_words = sc.broadcast(set(stop_words))

# Word count logic
word_count = (
    rdd.flatMap(lambda line: re.sub(r"[^a-zA-Z ]", "", line).lower().split())
       .filter(lambda word: word not in broadcast_stop_words.value)
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
)

# Print result
for word, count in word_count.collect():
    print(word, count)

spark.stop()
