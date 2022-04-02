from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StreamKafkaApp").master("local[*]").getOrCreate()

# .option("startingOffsets", "earliest")  => ALL MESSAGES
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "salesTopic") \
    .option("startingOffsets", "latest") \
    .load()

df.printSchema()

json_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

schema = StructType() \
    .add("country", StringType()) \
    .add("sales_count", IntegerType())

parsed_df = json_df.select(from_json(col("value"), schema).alias("sales_data"), "timestamp")
final_df = parsed_df.select("sales_data.*", "timestamp")

final_df.writeStream \
    .format("csv") \
    .option("startingOffsets", "earliest") \
    .option("path", "/myhdp/data/kafkastream") \
    .option("checkpointLocation", "/myhdp/data/checkpointdir") \
    .start()
