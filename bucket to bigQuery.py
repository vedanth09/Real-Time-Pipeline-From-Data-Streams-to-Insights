from google.cloud import storage
from google.cloud import bigquery
import os

# Set GCP configurations
PROJECT_ID = "data-engineering-444616"
BUCKET_NAME = "movies_cleaneddata"
BIGQUERY_DATASET = "movies_dataset"  # Valid dataset name with underscores
BIGQUERY_TABLE = "cleaned_fullmovies18"  # Valid table name
CSV_FILE_PATH = "/Users/vedanth/Desktop/projects/Movies API Pipeline/cleaned_movies07.csv"
GCS_FILE_NAME = "raw_movies2.csv"  # Name to save in the bucket

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the specified GCS bucket.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to GCS bucket {bucket_name} as {destination_blob_name}.")
    except Exception as e:
        print(f"Error uploading file to GCS: {e}")

def create_bigquery_table(project_id, dataset_name, table_name, schema):
    """
    Creates a BigQuery table if it does not exist.
    """
    try:
        bigquery_client = bigquery.Client(project=project_id)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        # Check if the table already exists
        try:
            bigquery_client.get_table(table_ref)  # Fetch the table
            print(f"Table {table_name} already exists in dataset {dataset_name}.")
        except Exception:
            # If not found, create the table
            table = bigquery.Table(table_ref, schema=schema)
            bigquery_client.create_table(table)
            print(f"Table {table_name} created in dataset {dataset_name}.")
    except Exception as e:
        print(f"Error creating BigQuery table: {e}")

def load_to_bigquery(project_id, bucket_name, gcs_file_name, dataset_name, table_name):
    """
    Loads a CSV file from GCS into a BigQuery table.
    """
    try:
        bigquery_client = bigquery.Client(project=project_id)

        # Define table schema explicitly
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("overview", "STRING"),
            bigquery.SchemaField("release_date", "DATE"),
            bigquery.SchemaField("runtime", "STRING"),
            bigquery.SchemaField("genres", "STRING"),
            bigquery.SchemaField("production_companies", "STRING"),
            bigquery.SchemaField("budget", "INTEGER"),
            bigquery.SchemaField("revenue", "INTEGER"),
            bigquery.SchemaField("popularity", "FLOAT"),
            bigquery.SchemaField("vote_average", "FLOAT"),
            bigquery.SchemaField("vote_count", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("poster_path", "STRING"),
            bigquery.SchemaField("backdrop_path", "STRING"),
            bigquery.SchemaField("language", "STRING"),
        ]

        # Create the table if it doesn't exist
        create_bigquery_table(project_id, dataset_name, table_name, schema)

        # Construct the table ID
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        # Load job configuration with explicit schema and error handling options
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip the header row
            schema=schema,        # Explicitly provide the schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=1000,  # Set max bad records to 1000 (ignore malformed rows)
            allow_jagged_rows=True  # Allow rows with different column counts
        )

        # URI of the CSV file in GCS
        uri = f"gs://{bucket_name}/{gcs_file_name}"

        # Load data from GCS into BigQuery
        load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        print(f"Data successfully loaded into BigQuery table {table_id}.")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")

if __name__ == "__main__":
    # Step 1: Upload CSV to GCS
    upload_to_gcs(BUCKET_NAME, CSV_FILE_PATH, GCS_FILE_NAME)

    # Step 2: Load data from GCS into BigQuery
    load_to_bigquery(PROJECT_ID, BUCKET_NAME, GCS_FILE_NAME, BIGQUERY_DATASET, BIGQUERY_TABLE)
