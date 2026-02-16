"""
Load taxi trip data into BigQuery for dbt Week 4 homework.
Creates external tables pointing to parquet files in GCS.
"""
import os
from google.cloud import storage, bigquery

# Setup
PROJECT_ID = "playground-486505"
BUCKET_NAME = "data-engineering-zoomcamp-2026"
DATASET_ID = "raw_nyc_tripdata"
CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "week3-dw-bigquery", "gcs.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

bq_client = bigquery.Client(project=PROJECT_ID)
gcs_client = storage.Client(project=PROJECT_ID)
bucket = gcs_client.bucket(BUCKET_NAME)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Data files to download and upload
FILES = {
    "green": [f"green_tripdata_2019-{m:02d}.parquet" for m in range(1, 13)]
           + [f"green_tripdata_2020-{m:02d}.parquet" for m in range(1, 13)],
    "yellow": [f"yellow_tripdata_2019-{m:02d}.parquet" for m in range(1, 13)]
            + [f"yellow_tripdata_2020-{m:02d}.parquet" for m in range(1, 13)],
    "fhv": [f"fhv_tripdata_2019-{m:02d}.parquet" for m in range(1, 13)],
}


def upload_to_gcs(file_name, prefix):
    """Download from TLC and upload to GCS."""
    import subprocess
    blob_name = f"week4/{prefix}/{file_name}"
    blob = bucket.blob(blob_name)

    if blob.exists():
        print(f"  Already exists: gs://{BUCKET_NAME}/{blob_name}")
        return

    url = f"{BASE_URL}/{file_name}"
    print(f"  Downloading {url}...")
    local_path = f"/tmp/{file_name}"
    subprocess.run(["curl", "-sL", "-o", local_path, url], check=True)

    print(f"  Uploading to gs://{BUCKET_NAME}/{blob_name}...")
    blob.upload_from_filename(local_path)
    os.remove(local_path)
    print(f"  Done: {blob_name}")


def create_external_table(table_name, gcs_prefix):
    """Create BigQuery external table pointing to GCS parquet files."""
    dataset_ref = bq_client.dataset(DATASET_ID)

    # Create dataset if not exists
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "us-west1"
        bq_client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")

    table_ref = dataset_ref.table(table_name)
    uri = f"gs://{BUCKET_NAME}/week4/{gcs_prefix}/*.parquet"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [uri]
    external_config.autodetect = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    # Delete if exists, then create
    try:
        bq_client.delete_table(table_ref)
    except Exception:
        pass

    bq_client.create_table(table)
    print(f"Created external table {DATASET_ID}.{table_name} -> {uri}")


if __name__ == "__main__":
    # Step 1: Upload parquet files to GCS
    for prefix, files in FILES.items():
        print(f"\n--- Uploading {prefix} taxi data ---")
        for f in files:
            upload_to_gcs(f, prefix)

    # Step 2: Create external tables
    print("\n--- Creating external tables ---")
    create_external_table("ext_green_taxi", "green")
    create_external_table("ext_yellow_taxi", "yellow")
    create_external_table("ext_fhv_taxi", "fhv")

    print("\nAll done!")
