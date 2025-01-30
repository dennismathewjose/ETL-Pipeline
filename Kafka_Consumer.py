from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofweek, avg, sum, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session with Kafka and BigQuery Connector JARs
spark = SparkSession.builder.master("local[*]") \
    .appName("BigQueryKafkaProcessing") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4," +
        "com.google.cloud.spark:spark-3.5-bigquery:0.41.1")\
    .config("temporaryGcsBucket", "nyc-taxidata-bucket") \
    .config("credentials", "./service_account_detail.json") \
    .getOrCreate()

# Define schema based on BigQuery table structure
schema = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Read Kafka Stream from the "nyc-taxi" topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "NYC-taxi-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the Kafka message (JSON)
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle missing values (replace nulls with default values)
df = df.fillna({
    "tpep_pickup_datetime": "1970-01-01 00:00:00",
    "tpep_dropoff_datetime": "1970-01-01 00:00:00",
    "passenger_count": 0,
    "trip_distance": 0.0,
    "total_amount": 0.0
})

# Data processing: Convert timestamps and extract features
df_processed = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                 .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
                 .withColumn("year", year(col("pickup_datetime"))) \
                 .withColumn("month", month(col("pickup_datetime"))) \
                 .withColumn("day_of_week", dayofweek(col("pickup_datetime")))

# Add watermark for late data
df_processed_with_watermark = df_processed.withWatermark("pickup_datetime", "15 minutes")

# Aggregation: Calculate average and total trip statistics per month
df_aggregated = df_processed_with_watermark.groupBy("year", "month") \
    .agg(
        avg("trip_distance").alias("avg_trip_distance"),
        sum("total_amount").alias("total_revenue"),
        count("*").alias("total_trips"),
        avg("passenger_count").alias("avg_passenger_count")
    )

# Remove unwanted columns or rename them to match the BigQuery table schema
df_cleaned = df_aggregated.selectExpr(
    "year AS year",
    "month AS month",
    "avg_trip_distance AS avg_trip_distance",
    "total_revenue AS total_revenue",
    "total_trips AS total_trips",
    "avg_passenger_count AS avg_passenger_count"
)

# Write the processed data to BigQuery
query = df_cleaned.writeStream \
    .format("bigquery") \
    .option("table", "fourth-stock-447916-u1.NYC_TaxiData.nyc-taxi-data-transformed") \
    .option("checkpointLocation", "/tmp/bq-checkpoints/") \
    .outputMode("update") \
    .start()

query.awaitTermination()
