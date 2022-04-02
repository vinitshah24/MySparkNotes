from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StreamApp").master("local[*]").getOrCreate()

df = spark \
    .readStream \
    .format("socket") \
    .option("host","localhost") \
    .option("port","9999") \
    .load()

print(df.isStreaming)
print(df.printSchema())

words_df = df.select(explode(split(df.value, " ")).alias("word"))
wc_df = words_df.groupBy("word").count()
query = wc_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

# wc_df = df.select(explode(split(df.value, " ")).alias("word"))
# query = wc_df \
#         .writeStream \
#         .format("parquet") \
#         .option("checkpointLocation", "tmp/checkpoint_data") \
#         .option("path", "tmp/country_data") \
#         .start()

# query.awaitTermination()
# query.stop()