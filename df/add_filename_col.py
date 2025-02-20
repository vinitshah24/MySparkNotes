from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, reverse, col


file_path = "data/fakefriends.csv"

spark = SparkSession.builder.appName("App").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema", True).load(file_path)
df = df.withColumn("src_file_path", input_file_name())
df = df.withColumn("filename", reverse(split(col("src_file_path"),"/")).getItem(0)).drop("src_file_path")
df = df.withColumn("state", split(df.filename, ".csv").getItem(0)).drop("filename")
df.show(5, False)

