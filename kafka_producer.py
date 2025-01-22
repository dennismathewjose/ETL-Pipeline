from google.cloud import bigquery
from confluent_kafka import Producer
import json
import os
from datetime import datetime
# Ensure Google credentials are set inside the container
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise Exception("Google credentials not set in environment variables")

# Initialize BigQuery client
client = bigquery.Client()

# BigQuery SQL query
query = """
SELECT * FROM `fourth-stock-447916-u1.NYC_TaxiData.nyc-taxi-data-table`
LIMIT 100
"""

#Test 
query_job = client.query(query)

# Kafka producer configuration
# Configure Kafka producer
kafka_config = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(kafka_config)
topic = 'NYC-taxi-topic'


# Function to convert row data to JSON-serializable format
def serialize_row(row):
    row_dict = dict(row)
    for key, value in row_dict.items():
        if isinstance(value, datetime):  # Convert datetime to string
            row_dict[key] = value.isoformat()
    return json.dumps(row_dict)
 
# Publish query results to Kafka
for row in query_job:
    message = serialize_row(row)
    producer.produce(topic, key=str(row[0]), value=message)
 
producer.flush()
print("Data pushed to Kafka successfully!")
