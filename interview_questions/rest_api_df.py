import json
import requests
from pyspark.sql.functions import udf, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
)
from pyspark.sql import SparkSession, Row


spark = SparkSession.builder.appName("REST_API_with_PySpark_DF").getOrCreate()


def executeRestApi(verb, url, body, page):
    """Function used by the UDF to execute HTTP requests"""
    headers = {"Content-Type": "application/json", "User-Agent": "apache spark 3.x"}
    res = None
    try:
        if verb == "get":
            res = requests.get(f"{url}/{page}", data=body, headers=headers)
        else:
            res = requests.post(f"{url}/{page}", data=body, headers=headers)
    except Exception as e:
        return e
    if res and res.status_code == 200:
        return json.loads(res.text)
    return None


schema = StructType([
    StructField("maxRecords", IntegerType(), True),
    StructField("results", ArrayType(
        StructType([
                StructField("Make_ID", IntegerType()),
                StructField("Make_Name", StringType()),
            ])
        ),
    ),
])

udf_executeRestApi = udf(executeRestApi, schema)

# create an iterator to download all the data from the web service
body = json.dumps({})
MAX_PAGE_SIZE = 100  # total number of records per page
MAX_PAGES = 20  # total number pages per batch
RestApiRequestRow = Row("verb", "url", "body", "page")

firstDataFrame = True
# continue to iterate fetching records until one of the two conditions described above is true
# (HTTP status is not 200 or the number of records returned is less than the expected record size)
while True:
    pageRequestArray = []
    for iPages in range(1, MAX_PAGES):
        pageRequestArray.append(RestApiRequestRow("GET", "<...>", body, iPages))
    request_df = spark.createDataFrame(pageRequestArray)
    result_df = request_df.withColumn(
        "result", udf_executeRestApi(col("verb"), col("url"), col("body"), col("page"))
    )

    # execute an action to fetch the web service data strip out any responses where the web service response are
    # not valid
    result_df = result_df.where("")

    # save the DataFrame to storage
    if firstDataFrame:
        result_df.write.format("parquet").save("tmp/webservice")
        firstDateFrame = False
    else:
        result_df.write.mode("append").format("parquet").save("tmp/webservice")

    # test the responses - do we break out of the iteration loop or continue
    if result_df.count() < MAX_PAGE_SIZE:
        break

# now select the complete response dataframe from storage and continue with your pipeline
complete_df = spark.read.format("parquet").load("tmp/webservice")
