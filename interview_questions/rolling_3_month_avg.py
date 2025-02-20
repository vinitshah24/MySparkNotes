from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, date_format
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("3-Month Rolling Average").getOrCreate()

data = [
    ("user_1", 100, "2024-01-15"),
    ("user_2", 150, "2024-01-20"),
    ("user_3", 200, "2024-02-18"),
    ("user_4", 250, "2024-02-25"),
    ("user_1", 300, "2024-03-10"),
    ("user_2", 350, "2024-03-20"),
    ("user_3", 400, "2024-04-01"),
    ("user_4", 450, "2024-04-15"),
]
df = spark.createDataFrame(data, ["user_id", "purchase_amount", "purchase_date"])

# Extract year and month from purchase_date
df = df.withColumn("year_month", date_format(col("purchase_date"), "yyyy-MM"))

# Aggregate revenue by year_month
monthly_revenue_df = (
    df
    .groupBy("year_month")
    .agg(sum("purchase_amount").alias("total_revenue"))
    .orderBy("year_month")
)

# Define window for 3-month rolling average (preceding 2 months + current month)
window_spec = Window.orderBy("year_month").rowsBetween(-2, 0)

# Calculate the 3-month rolling average
rolling_avg_df = monthly_revenue_df.withColumn(
    "3_month_rolling_avg", avg(col("total_revenue")).over(window_spec)
)

# Show the result
rolling_avg_df.orderBy("year_month").show()

