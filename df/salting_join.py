from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Salting Example").getOrCreate()

# Generate data for cities
city_data = ["Aurangabad"] * 10 + ["Mumbai"] * 3 + ["Chennai"] * 2 + ["Hyderabad"]

# Create DataFrame for cities
city_df = spark.createDataFrame([(city,) for city in city_data], ["city"])

# Add salt to city names
salted_city_df = city_df.withColumn("salt_key", F.explode(F.array(*[F.lit(i) for i in range(10)]))) \
                        .withColumn("city_salted", F.concat(F.col("city"), F.lit("_"), F.col("salt_key")))

salted_city_df.show(50)

# Generate data for states
state_data = [
    ("Aurangabad", "Maharashtra"),
    ("Mumbai", "Maharashtra"),
    ("Chennai", "TamilNadu"),
    ("Hyderabad", "Telangana")
]

# Create DataFrame for states
state_df = spark.createDataFrame(state_data, ["cityname", "state"])

# Add salt to city names in states
salted_state_df = state_df.withColumn(
    "cityname_salt_key",
    F.concat(F.col("cityname"), F.lit("_"), (F.floor(F.rand(123456) * 9)))
)

salted_state_df.show()

# Perform inner join between salted city names and salted city names in states
joined_df = salted_city_df.join(salted_state_df,
                                 salted_city_df["city_salted"] == salted_state_df["cityname_salt_key"],
                                 "inner")

# Select the desired columns from the joined DataFrame
final_df = joined_df.select("cityname", "state")

# Show the final result
final_df.show(truncate=False)

# Stop the Spark session
# spark.stop()
