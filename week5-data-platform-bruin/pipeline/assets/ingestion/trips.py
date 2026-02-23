"""@bruin
name: ingestion.trips
type: python

materialization:
  type: table

parameters:
  destination: duckdb-default

@bruin"""

import urllib.request
import duckdb

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green"]
MONTHS = range(1, 7)
YEAR = 2024


def ingest():
    conn = duckdb.connect("nyc_taxi.db")

    for taxi_type in TAXI_TYPES:
        for month in MONTHS:
            filename = f"{taxi_type}_tripdata_{YEAR}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            local_path = f"/tmp/{filename}"

            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(url, local_path)

            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS ingestion.{taxi_type}_trips AS
                SELECT * FROM read_parquet('{local_path}')
                WHERE 1=0;

                INSERT INTO ingestion.{taxi_type}_trips
                SELECT * FROM read_parquet('{local_path}');
            """)
            print(f"Loaded {filename} into ingestion.{taxi_type}_trips")

    conn.close()


ingest()
