import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType)


spark = SparkSession.builder.master("local").appName("StockAnalysis").getOrCreate()

STREAMING_DIR = "D:\BigData\Spark\streaming\directory\streaming_dir\stocks_data"
STATIC_DATA = "D:\BigData\Spark\streaming\directory\streaming_dir\company.txt"

# Date,Open,High,Low,Close,Volume,Name
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Name", StringType(), True),
])

spark.udf.register("get_stock_name_udf", lambda path: upper(os.path.basename(path).split("_")[0]))

df = spark \
    .readStream \
    .option("maxFilesPerTrigger", 2) \
    .option("header", True) \
    .schema(schema) \
    .csv(STREAMING_DIR)

df.printSchema()
print(f"Is Streaming: {df.isStreaming}")

stream_df = df.groupBy(col("Name"), year(col("Date")).alias("Year")).agg(max("High").alias("Max"))
stream_df.printSchema()

company_df = spark.read.option("header", True).csv(STATIC_DATA)
company_df.printSchema()
company_df.show()

# INNER JOIN
inner_df = company_df.join(stream_df, "Name", "inner")
inner_df.writeStream \
        .outputMode("complete") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .format("console") \
        .start() \
        .awaitTermination()
