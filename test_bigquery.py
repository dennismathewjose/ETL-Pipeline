from google.cloud import bigquery
 
# Instantiate the BigQuery client
client = bigquery.Client()
 
# Corrected query using valid columns from the Shakespeare dataset
query = """
    SELECT corpus, word_count
    FROM `bigquery-public-data.samples.shakespeare`
    LIMIT 5
"""
 
# Run the query
query_job = client.query(query)
 
# Wait for the job to finish
results = query_job.result()
 
# Check if results are returned and print them
if results.total_rows > 0:
    for row in results:
        print(f"{row['corpus']}: {row['word_count']}")
else:
    print("No results found.")
