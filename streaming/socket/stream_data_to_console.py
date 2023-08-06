import sys
import os

# os.environ.get('JAVA_HOME')
# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StreamApp").master("local[*]").getOrCreate()
spark.sparkContext.setCheckpointDir("temp/checkpoint")

df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9999") \
    .load()

print(df.isStreaming)
df.printSchema()

words_df = df.select(explode(split(df.value, " ")).alias("word"))
wc_df = words_df.groupBy("word").count()
query = wc_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Run to stop the streaming
query.stop()


# wc_df = df.select(explode(split(df.value, " ")).alias("word"))
# query = wc_df \
#         .writeStream \
#         .format("parquet") \
#         .option("checkpointLocation", "tmp/checkpoint_data") \
#         .option("path", "tmp/country_data") \
#         .start()

# query.awaitTermination()
# query.stop()

"""
Complete Mode:
The entire updated Result Table will be written to the external storage.
It is up to the storage connector to decide how to handle writing of the entire table.

Append Mode:
Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
This is applicable only on the queries where existing rows in the Result Table are not expected to change.

Update Mode:
Only the rows that were updated in the Result Table since the last trigger will be written to the external storage.
Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since
the last trigger. If the query doesn't contain aggregations, it will be equivalent to Append mode.
"""