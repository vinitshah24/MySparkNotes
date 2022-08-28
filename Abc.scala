
object Abc {

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().master("local[1]").appName("App").getOrCreate();

    val df = spark.createDataFrame(List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
    df.write.mode("overwrite").saveAsTable("test_db.hive_table")

    val df = spark.read.option("header",true).csv("data/zipcodes.csv")
    df.write.options("header",true).csv("/tmp/spark_output/zipcodes")

    val schema = new StructType()
        .add("RecordNumber",IntegerType,true)
        .add("Zipcode",IntegerType,true)
        .add("ZipCodeType",StringType,true)
        .add("Lat",DoubleType,true)
        .add("Decommisioned",BooleanType,true)
    val df_with_schema = spark.read.format("csv").option("header", "true").schema(schema).load("data/zipcodes.csv")

    // [{
    //   "RecordNumber": 2,
    //   "Zipcode": 704,
    //   "ZipCodeType": "STANDARD",
    //   "City": "PASEO COSTA DEL SUR",
    //   "State": "PR"
    // },
    // {
    //   "RecordNumber": 10,
    //   "Zipcode": 709,
    //   "ZipCodeType": "STANDARD",
    //   "City": "BDA SAN LUIS",
    //   "State": "PR"
    // }]
    val multiline_df = spark.read.option("multiline","true").json("src/main/resources/multiline-zipcode.json")
    multiline_df.show(false)

    val df = spark.read.format("orc").option("inferSchema", true).load("data.orc")

    val dfCache = df.cache()
    dfCache.show(false)

    import org.apache.spark.storage.StorageLevel
    val dfPersist = df.persist(StorageLevel.MEMORY_ONLY)
    dfPersist.show(false)

    dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
    rdd  = spark.sparkContext.parallelize(dept)
    df   = rdd.toDF()
    rdd  = df.rdd
}
