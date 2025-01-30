from google.cloud import storage

# Initialize a storage client
client = storage.Client.from_service_account_json("/Users/dennis_m_jose/Downloads/RealTimeNYCData/ProjectFiiles/service_account_detail.json")

# Test access to the bucket
bucket_name = "nyc-taxidata-bucket"
bucket = client.bucket(bucket_name)
if bucket.exists():
    print(f"Bucket {bucket_name} exists and is accessible.")
else:
    print(f"Bucket {bucket_name} does not exist or is not accessible.")