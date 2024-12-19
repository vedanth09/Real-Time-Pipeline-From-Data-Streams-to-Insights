from api_to_bucket import fetch_and_upload_movies
from gcs_to_bq import process_and_append_data

def main():
# Step 1: Fetch and upload movies data
    print("API fetching starts")
    fetch_and_upload_movies()
    
    # Step 2: Process and append data to BigQuery
    # You can modify this line to pass the correct file name or dataset
    process_and_append_data("movies_data_2024-12-01_to_2024-12-31.json")
    Print("Data is been stored in the bigquery")

if __name__=="__main__":
   main()