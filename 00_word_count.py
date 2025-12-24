from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Read text file
rdd = spark.sparkContext.textFile("wordcnt.txt")

# Word count logic
word_counts = (
    rdd.flatMap(lambda line: line.split(" "))
       .map(lambda word: (word.lower(), 1))
       .reduceByKey(lambda a, b: a + b)
)

# Print result
for word, count in word_counts.collect():
    print(word,"=", count)

spark.stop()
