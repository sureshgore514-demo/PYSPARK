from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("No-Warn") \
    .getOrCreate()

# Stop WARN logs
spark.sparkContext.setLogLevel("ERROR")

emp = spark.createDataFrame([
    (1,"Suresh",10),
    (2,"Amit",20),
    (3,"Ravi",30),
    (4,"Neha",40)
], ["emp_id","name","dept_id"])

dept = spark.createDataFrame([
    (10,"IT"),
    (20,"HR"),
    (30,"Finance")
], ["dept_id","dept_name"])

# inner join:

# emp.join(dept, "dept_id", "inner").show()

emp.join(dept, "dept_id", "left_anti").show()
