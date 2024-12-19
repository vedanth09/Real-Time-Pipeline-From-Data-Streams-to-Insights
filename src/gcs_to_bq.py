from google.cloud import storage, bigquery
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

# Download JSON data from GCS
def download_from_gcs(bucket_name, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        print(f"Data successfully downloaded from GCS: {file_name}")
        return json.loads(content)
    except Exception as e:
        print(f"Error downloading from GCS: {e}")
        return []

# Recursively remove 'adult' field from nested structures
def remove_unwanted_fields(movie):
    # If it's a dictionary, iterate over it
    if isinstance(movie, dict):
        # Remove the 'adult' field if present
        movie.pop('adult', None)
        # Recursively clean all nested dictionaries or lists
        for key, value in movie.items():
            movie[key] = remove_unwanted_fields(value)
    # If it's a list, iterate over its elements
    elif isinstance(movie, list):
        for i in range(len(movie)):
            movie[i] = remove_unwanted_fields(movie[i])
    return movie

# Clean the data
def clean_data(raw_data):
    cleaned_data = []
    for movie in raw_data:
        movie = remove_unwanted_fields(movie)  # Remove 'adult' field from nested data
        if all(movie.get(key) for key in ["id", "title", "release_date"]):  # Basic validation
            movie['runtime'] = f"{movie.get('runtime', 0)} minutes"
            cleaned_data.append(movie)
    print(f"Cleaned {len(cleaned_data)} records")
    return cleaned_data

# Append to BigQuery
def append_to_bigquery(dataset_name, table_name, data):
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{dataset_name}.{table_name}"

    # Check for duplicates
    table = bigquery_client.get_table(table_id)
    existing_ids = {row.id for row in bigquery_client.list_rows(table)}

    new_data = [record for record in data if record['id'] not in existing_ids]
    if not new_data:
        print("No new data to append. All records already exist.")
        return

    # Load new data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = bigquery_client.load_table_from_json(new_data, table_id, job_config=job_config)
    job.result()
    print(f"Appended {len(new_data)} records to BigQuery.")

# Main function
def process_and_append_data(file_name):
    raw_data = download_from_gcs(BUCKET_NAME, file_name)
    if raw_data:
        cleaned_data = clean_data(raw_data)
        append_to_bigquery(BIGQUERY_DATASET, BIGQUERY_TABLE, cleaned_data)

if __name__ == "__main__":
    # Replace with your GCS file name
    process_and_append_data("movies_data_2024-12-01_to_2024-12-31.json")
