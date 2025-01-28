FROM bitnami/spark:latest

# Install required BigQuery packages
RUN pip install google
RUN pip install google-cloud-bigquery pandas pyarrow confluent-kafka pyspark


# Set environment variable for authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service_account_detail.json"

# Copy service account key into container
COPY ./service_account_detail.json /app/service_account_detail.json

# Copy the producer and consumer Python scripts into the container
COPY ./kafka_producer.py /app/kafka_producer.py
COPY ./Kafka_Consumer.py /app/Kafka_Consumer.py
COPY ./wait_for_kafka.py /app/wait_for_kafka.py

# Set working directory
WORKDIR /app
 