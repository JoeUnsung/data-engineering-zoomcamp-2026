"""
Fix parquet type mismatches by reading with pyarrow, casting columns, and re-uploading to GCS.
Then load into BigQuery as native tables.
"""
import os
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage, bigquery

PROJECT_ID = "playground-486505"
BUCKET_NAME = "data-engineering-zoomcamp-2026"
DATASET_ID = "raw_nyc_tripdata"
CREDS = os.path.join(os.path.dirname(os.path.dirname(__file__)), "week3-dw-bigquery", "gcs.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS

gcs = storage.Client(project=PROJECT_ID)
bq = bigquery.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)


def fix_parquet_types(local_path, cast_map):
    """Read parquet, cast specified columns to float64, write back."""
    table = pq.read_table(local_path)
    for col_name, target_type in cast_map.items():
        if col_name in table.column_names:
            idx = table.column_names.index(col_name)
            col = table.column(idx)
            if col.type != target_type:
                table = table.set_column(idx, col_name, col.cast(target_type))
    pq.write_table(table, local_path)


def download_fix_reupload(prefix, file_list, cast_map):
    """Download, fix types, re-upload."""
    for f in file_list:
        blob_name = f"week4/{prefix}/{f}"
        fixed_blob_name = f"week4/{prefix}_fixed/{f}"
        fixed_blob = bucket.blob(fixed_blob_name)

        if fixed_blob.exists():
            print(f"  Already fixed: {f}")
            continue

        local_path = f"/tmp/{f}"
        print(f"  Fixing {f}...")
        bucket.blob(blob_name).download_to_filename(local_path)
        fix_parquet_types(local_path, cast_map)
        fixed_blob.upload_from_filename(local_path)
        os.remove(local_path)


def load_bq_table(table_name, gcs_prefix):
    """Load all fixed parquet files into a native BQ table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    bq.delete_table(table_id, not_found_ok=True)

    uri = f"gs://{BUCKET_NAME}/week4/{gcs_prefix}/*.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    print(f"  Loading {uri} -> {table_id}...")
    job = bq.load_table_from_uri(uri, table_id, job_config=job_config)
    job.result()
    t = bq.get_table(table_id)
    print(f"  Loaded {t.num_rows} rows, schema: {[s.name for s in t.schema]}")


if __name__ == "__main__":
    green_files = [f"green_tripdata_{y}-{m:02d}.parquet" for y in [2019, 2020] for m in range(1, 13)]
    yellow_files = [f"yellow_tripdata_{y}-{m:02d}.parquet" for y in [2019, 2020] for m in range(1, 13)]
    fhv_files = [f"fhv_tripdata_2019-{m:02d}.parquet" for m in range(1, 13)]

    print("=== Fixing Green taxi ===")
    download_fix_reupload("green", green_files, {
        "VendorID": pa.float64(),
        "RatecodeID": pa.float64(),
        "PULocationID": pa.float64(),
        "DOLocationID": pa.float64(),
        "passenger_count": pa.float64(),
        "payment_type": pa.float64(),
        "trip_type": pa.float64(),
        "ehail_fee": pa.float64(),
        "congestion_surcharge": pa.float64(),
    })

    print("\n=== Fixing Yellow taxi ===")
    download_fix_reupload("yellow", yellow_files, {
        "VendorID": pa.float64(),
        "RatecodeID": pa.float64(),
        "PULocationID": pa.float64(),
        "DOLocationID": pa.float64(),
        "passenger_count": pa.float64(),
        "payment_type": pa.float64(),
        "airport_fee": pa.float64(),
        "congestion_surcharge": pa.float64(),
    })

    print("\n=== Fixing FHV taxi ===")
    download_fix_reupload("fhv", fhv_files, {
        "PUlocationID": pa.float64(),
        "DOlocationID": pa.float64(),
        "SR_Flag": pa.float64(),
    })

    print("\n=== Loading into BigQuery ===")
    load_bq_table("ext_green_taxi", "green_fixed")
    load_bq_table("ext_yellow_taxi", "yellow_fixed")
    load_bq_table("ext_fhv_taxi", "fhv_fixed")

    # Grant dbt SA access
    from google.cloud.bigquery import AccessEntry
    dataset = bq.get_dataset(DATASET_ID)
    access_entries = list(dataset.access_entries)
    new_entry = AccessEntry(
        role="READER",
        entity_type="userByEmail",
        entity_id="dbt-bigquery-service-account-z@playground-486505.iam.gserviceaccount.com",
    )
    if new_entry not in access_entries:
        access_entries.append(new_entry)
        dataset.access_entries = access_entries
        bq.update_dataset(dataset, ["access_entries"])

    print("\nAll done!")
