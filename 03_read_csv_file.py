from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("csv_file") .master("local[*]") .enableHiveSupport()  \
    .getOrCreate()

df = spark.read.csv(
    "data.csv",
    header=True,
    inferSchema=True
)

df.show()
