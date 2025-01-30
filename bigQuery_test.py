from google.cloud import bigquery
from google.oauth2 import service_account

# Path to your service account key file
key_path = "/Users/dennis_m_jose/Downloads/RealTimeNYCData/ProjectFiiles/service_account_detail.json"

# Create credentials
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Test query
query = """
SELECT * FROM `fourth-stock-447916-u1.NYC_TaxiData.nyc-taxi-data-table` LIMIT 10
"""
query_job = client.query(query)
results = query_job.result()

if results == None:
    print("the table is empty")
else:
    for row in results:
        print(row)