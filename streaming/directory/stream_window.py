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

# Create Streaming DataFrame by reading data from directory.
# This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
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

# Display Data to Console with Aggregation.
# ------------------------------------------------------------------------------

# Tumbling window: where the 2 consecutive windows are non-overlapping.
tumbling_df = df \
    .select("Name", "Date", "Open", "High", "Low") \
    .groupBy(window(col("Date"), "10 days"), col("Name")) \
    .agg(max("High").alias("Max")) \
    .orderBy(col("window.start"))

print("Tumbling Window DF Schema:")
tumbling_df.printSchema()

tumbling_df \
    .writeStream \
    .outputMode("complete") \
    .option("truncate", False) \
    .format("console") \
    .start() \
    .awaitTermination()

# ------------------------------------------------------------------------------

# Sliding window: where windows will be overlapping.
# 5 days => slide duration & window size = 10 days

sliding_window_df = df \
    .select("Name", "Date", "Open", "High", "Low") \
    .groupBy(window(col("Date"), "10 days", slideDuration="5 days"), col("Name")) \
    .agg(max("High").alias("Max")) \
    .orderBy(col("window.start"))

print("Sliding Window DF Schema:")
sliding_window_df.printSchema()

sliding_window_df \
    .writeStream \
    .outputMode("complete") \
    .option("truncate", False) \
    .format("console") \
    .start() \
    .awaitTermination()

# ------------------------------------------------------------------------------

# Append mode doesn't allow aggregations
df \
    .writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .start() \
    .awaitTermination()
