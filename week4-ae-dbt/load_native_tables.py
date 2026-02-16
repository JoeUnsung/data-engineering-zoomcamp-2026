"""
Load taxi data as native BigQuery tables file by file to handle type mismatches.
"""
import os
from google.cloud import bigquery

PROJECT_ID = "playground-486505"
BUCKET_NAME = "data-engineering-zoomcamp-2026"
DATASET_ID = "raw_nyc_tripdata"
CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "week3-dw-bigquery", "gcs.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
client = bigquery.Client(project=PROJECT_ID)


def load_table_file_by_file(table_name, gcs_prefix, file_list, schema):
    """Load parquet files one by one into a native BigQuery table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Delete existing
    client.delete_table(table_id, not_found_ok=True)

    for i, file_name in enumerate(file_list):
        uri = f"gs://{BUCKET_NAME}/week4/{gcs_prefix}/{file_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            schema=schema,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ],
        )

        try:
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            print(f"  OK: {file_name}")
        except Exception as e:
            print(f"  FAIL: {file_name} - {e}")
            # Try without schema for this file
            job_config2 = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            try:
                load_job2 = client.load_table_from_uri(uri, table_id, job_config=job_config2)
                load_job2.result()
                print(f"  OK (retry without schema): {file_name}")
            except Exception as e2:
                print(f"  FAIL (retry): {file_name} - {e2}")

    table = client.get_table(table_id)
    print(f"  Total rows: {table.num_rows}")


if __name__ == "__main__":
    green_files = [f"green_tripdata_{y}-{m:02d}.parquet" for y in [2019, 2020] for m in range(1, 13)]
    yellow_files = [f"yellow_tripdata_{y}-{m:02d}.parquet" for y in [2019, 2020] for m in range(1, 13)]
    fhv_files = [f"fhv_tripdata_2019-{m:02d}.parquet" for m in range(1, 13)]

    print("--- Green taxi ---")
    load_table_file_by_file("ext_green_taxi", "green", green_files, None)

    print("\n--- Yellow taxi ---")
    load_table_file_by_file("ext_yellow_taxi", "yellow", yellow_files, None)

    print("\n--- FHV taxi ---")
    load_table_file_by_file("ext_fhv_taxi", "fhv", fhv_files, None)

    # Grant dbt SA access
    from google.cloud.bigquery import AccessEntry
    dataset = client.get_dataset(DATASET_ID)
    access_entries = list(dataset.access_entries)
    new_entry = AccessEntry(
        role="READER",
        entity_type="userByEmail",
        entity_id="dbt-bigquery-service-account-z@playground-486505.iam.gserviceaccount.com",
    )
    if new_entry not in access_entries:
        access_entries.append(new_entry)
        dataset.access_entries = access_entries
        client.update_dataset(dataset, ["access_entries"])

    print("\nAll done!")
