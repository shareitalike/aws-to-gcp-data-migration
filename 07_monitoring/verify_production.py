from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.env'))
client = bigquery.Client()
query = "SELECT count(*) as count FROM analytics.enriched_orders WHERE process_date = '2026-03-19'"
print(f"PIPELINE_VERIFICATION: {list(client.query(query).result())[0].count} rows found in production table!")
