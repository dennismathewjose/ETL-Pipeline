FROM bitnami/spark:3.4.0

# Install required Python packages for BigQuery, Kafka, and Spark
RUN pip install google-cloud-bigquery pandas pyarrow confluent-kafka pyspark

# Set environment variable for Google Cloud authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service_account_detail.json"

# Copy service account key into container
COPY service_account_detail.json /app/service_account_detail.json

# Copy the producer and consumer Python scripts into the container
COPY ./wait_for_kafka.py /app/wait_for_kafka.py
COPY ./kafka_producer.py /app/kafka_producer.py
COPY ./Kafka_Consumer.py /app/Kafka_Consumer.py

# Set working directory
WORKDIR /app
