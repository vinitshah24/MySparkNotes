import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType)


spark = SparkSession.builder.master("local").appName("StockAnalysis").getOrCreate()

STREAMING_DIR = "D:\BigData\Spark\streaming\directory\streaming_dir\stocks_data"

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adjusted Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True)
])

spark.udf.register("get_stock_name_udf", lambda path: os.path.basename(path).split("_")[0])

df = spark \
    .readStream \
    .option("maxFilesPerTrigger", 2) \
    .option("header", True) \
    .schema(schema) \
    .csv(STREAMING_DIR) \
    .withColumn("Name", call_udf("get_stock_name_udf", input_file_name())) \
    .select("Name", "Date", "Open", "High", "Low", "Close", "Adjusted Close", "Volume")

df.printSchema()
print(f"Is Streaming: {df.isStreaming}")

# ex = expr("IF((Close - Open) > 0, 'UP', 'DOWN')")
spark.udf.register("get_stock_trend_udf", lambda open, close: "Up" if close - open > 0 else "Down")

df.createOrReplaceTempView("stream_view")
result_df = spark.sql("SELECT *, get_stock_trend_udf(Open, Close) AS trend FROM stream_view")

result_df \
  .writeStream \
  .outputMode("append") \
  .option("truncate", False) \
  .format("console") \
  .start() \
  .awaitTermination()
