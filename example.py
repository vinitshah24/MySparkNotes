import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

spark = SparkSession.builder.appName('ReadWrite').getOrCreate()
words = spark.sparkContext.textFile("/input.txt").flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
wordCounts.saveAsTextFile("/output/")
output = wordCounts.collect()
for (word, count) in output: print(word, count)

# Accumulator
bad_record_acc = spark.sparkContext.accumulator(0)

# Broadcast variable
prod_dict = {'4123': 'Prod1', '6124': 'Prod2', '5125': 'Prod3'}
prod_bc = spark.sparkContext.broadcast(prod_dict)

# Remove header from df
df = spark.read.text("path")
header = df.first()[0]
df.filter(~col("value").contains(header)).show(10, False)

# Cache & Persist
df.cache()
df.persist(pyspark.StorageLevel.MEMORY_ONLY)
df.unpersist()

rdd = df.rdd
rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
rdd.unpersist()

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd  = spark.sparkContext.parallelize(dept)
df   = rdd.toDF()

# ----------------------------------------------------------

df = spark.read.options(header=True, delimiter=',').option("inferSchema", True).csv("/tmp/zipcodes.csv")
df.printSchema()

schema = StructType() \
    .add("RecordNumber", IntegerType(), True) \
    .add("Zipcode", IntegerType(), True) \
    .add("ZipCodeType", StringType(), True) \
    .add("City", StringType(), True) \
    .add("State", StringType(), True) \
    .add("LocationType", StringType(), True) \
    .add("Lat", DoubleType(), True) \
    .add("Long", DoubleType(), True) \
    .add("Xaxis", IntegerType(), True) \
    .add("Yaxis", DoubleType(), True) \
    .add("Zaxis", DoubleType(), True) \
    .add("WorldRegion", StringType(), True) \
    .add("Country", StringType(), True) \
    .add("LocationText", StringType(), True) \
    .add("Location", StringType(), True) \
    .add("Decommisioned", BooleanType(), True) \
    .add("TaxReturnsFiled", StringType(), True) \
    .add("EstimatedPopulation", IntegerType(), True) \
    .add("TotalWages", IntegerType(), True) \
    .add("Notes", StringType(), True)

df_with_schema = spark.read.format("csv").option("header", True).schema(schema).load("/tmp/zipcodes.csv")
df_with_schema.printSchema()

df.write.option("header", True).csv("/tmp/spark_output/zipcodes123")

# ---------------------------------------------------------------------------------------------

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
rdd = spark.sparkContext.parallelize(dept)
deptColumns = ["dept_name", "dept_id"]

df = rdd.toDF(deptColumns)
df.printSchema()
df.show(truncate=False)

deptDF = spark.createDataFrame(rdd, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# ---------------------------------------------------------------------------------------------

deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
