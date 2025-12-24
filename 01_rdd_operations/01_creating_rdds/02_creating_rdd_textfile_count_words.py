# You can create an RDD from an external dataset such as a local file system or HDFS using
# the textFile() method, which reads files as RDDs of text lines.

from pyspark import SparkContext

sc = SparkContext("local", "TextFileExample")
rdd = sc.textFile(r"file:///C:\DEVELOPMENT\PYSPARK\wordcount.txt")
# print(rdd.collect())

word_count = (
    rdd.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

# print results
for word, count in word_count.collect():
    print(word, count)
sc.stop()
