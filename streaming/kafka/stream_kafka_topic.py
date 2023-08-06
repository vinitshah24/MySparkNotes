from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

stream_location = "/myhdp/data/kafkastream"
checkpoint_location = "/myhdp/data/checkpointdir"

spark = SparkSession.builder.appName("StreamKafkaApp").master("local[*]").getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "salesTopic") \
    .option("startingOffsets", "latest") \
    .load()
# .option("startingOffsets", "earliest")  => ALL MESSAGES

df.printSchema()

json_df = df.selectExpr("CAST(value AS STRING)", "timestamp")
schema = StructType().add("country", StringType()).add("sales_count", IntegerType())
parsed_df = json_df.select(from_json(col("value"), schema).alias("sales_data"), "timestamp")
final_df = parsed_df.select("sales_data.*", "timestamp")

final_df.writeStream \
    .format("csv") \
    .option("startingOffsets", "earliest") \
    .option("path", stream_location) \
    .option("checkpointLocation", checkpoint_location) \
    .start()
