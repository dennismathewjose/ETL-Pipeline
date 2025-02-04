from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofweek, avg, sum, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create Spark Session without using spark.jars.packages
spark = SparkSession.builder.master("local[*]") \
    .appName("BigQueryKafkaProcessing") \
    .getOrCreate()

# Schema remains the same
schema = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Read from Kafka with explicit value deserialization
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "NYC-taxi-topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(col("value").cast("string").alias("value"))

# Parse JSON with explicit error handling
parsed_df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Handle missing values
parsed_df = parsed_df.fillna({
    "tpep_pickup_datetime": "1970-01-01 00:00:00",
    "tpep_dropoff_datetime": "1970-01-01 00:00:00",
    "passenger_count": 0,
    "trip_distance": 0.0,
    "total_amount": 0.0
})

# Process data
processed_df = parsed_df \
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
    .withColumn("year", year(col("pickup_datetime"))) \
    .withColumn("month", month(col("pickup_datetime"))) \
    .withColumn("day_of_week", dayofweek(col("pickup_datetime")))

# Apply watermark
processed_df = processed_df.withWatermark("pickup_datetime", "1 minute")

# Aggregation
aggregated_df = processed_df.groupBy("year", "month") \
    .agg(
        avg("trip_distance").alias("avg_trip_distance"),
        sum("total_amount").alias("total_revenue"),
        count("*").alias("total_trips"),
        avg("passenger_count").alias("avg_passenger_count")
    )

# Write to BigQuery with modified options


query = aggregated_df.writeStream \
    .format("bigquery") \
    .option("table", "fourth-stock-447916-u1.NYC_TaxiData.nyc-taxi-data-transformed") \
    .option("temporaryGcsBucket", "nyc-taxidata-bucket") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("credentialsFile", "/app/service_account_detail.json") \
    .outputMode("complete") \
    .start()

query.awaitTermination()
