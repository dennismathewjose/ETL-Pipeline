from pyspark.sql import SparkSession

# Initialize Spark Session with BigQuery Connector
spark = SparkSession.builder.master("local[*]") \
    .appName("BigQueryTest") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.41.1") \
    .config("temporaryGcsBucket", "nyc-taxidata-bucket") \
    .getOrCreate()

# Create a simple DataFrame
df_test = spark.createDataFrame(
    [(2023, 10, 5.5, 100.0, 10, 2.5)],
    ["year", "month", "avg_trip_distance", "total_revenue", "total_trips", "avg_passenger_count"]
)

# Write the DataFrame to BigQuery
df_test.write \
    .format("bigquery") \
    .option("table", "your-project-id.your_dataset.your_table") \
    .option("temporaryGcsBucket", "nyc-taxidata-bucket") \
    .mode("append") \
    .save()

print("Data successfully written to BigQuery!")