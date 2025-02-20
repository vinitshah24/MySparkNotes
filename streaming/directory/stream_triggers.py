import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType)
# from pyspark.sql.streaming import Trigger


spark = SparkSession.builder.master("local").appName("StockAnalysis").getOrCreate()

STREAMING_DIR = "D:\BigData\Spark\streaming\directory\streaming_dir\stocks_data"
CHECKPOINT_DIR = "D:\BigData\Spark\streaming\directory\checkpoint_dir"

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adjusted Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True)
])

spark.udf.register("get_stock_name_udf",
                   lambda path: os.path.basename(path).split("_")[0])

df = spark \
    .readStream \
    .option("maxFilesPerTrigger", 1) \
    .option("header", True) \
    .schema(schema) \
    .csv(STREAMING_DIR) \
    .withColumn("Name", call_udf("get_stock_name_udf", input_file_name())) \
    .withColumn("Timestamp", current_timestamp()) \
    .select("Name", "Date", "Open", "High", "Low", "Close", "Adjusted Close", "Volume", "Timestamp")

df.printSchema()
print(f"Is Streaming: {df.isStreaming}")

result_df = df \
    .groupBy(col("Name"), year(col("Date")).alias("Year")) \
    .agg(max("High").alias("Max"), max("Timestamp").alias("Timestamp")) \
    .orderBy(col("Timestamp").desc)

# Checkpointing
result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start() \
    .awaitTermination()


# Default:
#     Executes a micro-batch as soon as the previous finishes.

# Fixed interval micro-batches:
#     Specifies the interval when the micro-batches will execute.
#     E.g., 1 minute, 30 seconds or 1 hour, etc. If the processing time of the previous batch is more than the
#     specified interval, the next batch will be executed immediately.
#     E.g., If we set the processing time as 1 minute, if the micro-batch takes 35 seconds,
#     it will wait for more than 25 seconds before triggering the next batch.
#     If the micro-batch takes 70 secs, then next will be executed immediately.

# One-time micro-batch:
#     Executes only one micro-batch to process all available data and then stops.
#     With a once trigger "trigger(Trigger.Once())", our query will execute a single micro-batch.
#     It will process all available data and then stop the application.
#     This trigger is useful when you would like to spin up a cluster periodically, process all available data,
#     and then shut down the cluster. This may lead to considerable cost savings.

# By default, if we don't specify any trigger, our query will execute in micro-batch mode.
# The default trigger executes the next batch as soon as the previous one finishes.
# In our checkpoint example, we used the default trigger since we hadn't specified another.
# Here we set 1 minute for each micro-batch. And if you observe the interval for batch-01 and batch-02, likely 1 minute.


# # Fixed interval micro-batches
# result_df.writeStream \
#       .outputMode("complete") \
#       .trigger(Trigger.ProcessingTime("1 minute")) \
#       .format("console") \
#       .option("truncate", False) \
#       .start() \
#       .awaitTermination()

# # One-time micro-batch
# result_df.writeStream \
#       .outputMode("complete") \
#       .trigger(Trigger.Once(), Trigger.ProcessingTime("1 minute")) \
#       .format("console") \
#       .option("truncate", False) \
#       .start() \
#       .awaitTermination()
