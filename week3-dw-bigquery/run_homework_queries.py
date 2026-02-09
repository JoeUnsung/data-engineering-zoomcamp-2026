import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# GCS 인증 파일 설정
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcs.json'

# BigQuery 클라이언트 생성
client = bigquery.Client()

# 프로젝트 ID와 데이터셋 ID
PROJECT_ID = client.project
DATASET_ID = "taxi_data"
BUCKET_NAME = "data-engineering-zoomcamp-2026"

print(f"Project ID: {PROJECT_ID}")
print(f"Dataset ID: {DATASET_ID}")
print("=" * 80)

# 데이터셋 생성
def create_dataset():
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-west1"

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Dataset {dataset_id} created or already exists\n")
    except Exception as e:
        print(f"Error creating dataset: {e}\n")

# External Table 생성
def create_external_table():
    table_id = f"{PROJECT_ID}.{DATASET_ID}.yellow_taxi_external"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"gs://{BUCKET_NAME}/yellow_tripdata_2024-*.parquet"]

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    try:
        table = client.create_table(table, exists_ok=True)
        print(f"✓ External table {table_id} created\n")
    except Exception as e:
        print(f"Error creating external table: {e}\n")

# Materialized Table 생성
def create_materialized_table():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized` AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_external`
    """

    try:
        query_job = client.query(query)
        query_job.result()
        print(f"✓ Materialized table created\n")
    except Exception as e:
        print(f"Error creating materialized table: {e}\n")

# 쿼리 실행 및 결과 출력 (예상 바이트 포함)
def run_query_with_stats(question_num, description, query):
    print(f"\n{'='*80}")
    print(f"Question {question_num}: {description}")
    print(f"{'='*80}")

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    try:
        # Dry run으로 예상 바이트 확인
        dry_run_job = client.query(query, job_config=job_config)
        estimated_bytes = dry_run_job.total_bytes_processed
        estimated_mb = estimated_bytes / (1024 * 1024)
        estimated_gb = estimated_bytes / (1024 * 1024 * 1024)

        if estimated_gb >= 1:
            print(f"Estimated bytes to process: {estimated_gb:.2f} GB ({estimated_bytes:,} bytes)")
        else:
            print(f"Estimated bytes to process: {estimated_mb:.2f} MB ({estimated_bytes:,} bytes)")

        # 실제 쿼리 실행
        query_job = client.query(query)
        results = query_job.result()

        print(f"\nResults:")
        for row in results:
            print(f"  {dict(row)}")

        print(f"\nActual bytes processed: {query_job.total_bytes_processed:,}")

    except Exception as e:
        print(f"Error: {e}")

# Main execution
if __name__ == "__main__":
    # 1. 데이터셋 생성
    create_dataset()

    # 2. External Table 생성
    create_external_table()

    # 3. Materialized Table 생성
    create_materialized_table()

    # Question 1: 2024 Yellow Taxi 데이터의 레코드 수
    query1 = f"""
    SELECT COUNT(*) AS record_count
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    """
    run_query_with_stats(1, "Total record count for 2024 Yellow Taxi Data", query1)

    # Question 2: External Table vs Materialized Table 예상 데이터 읽기량
    print(f"\n{'='*80}")
    print(f"Question 2: Estimated bytes - External Table vs Materialized Table")
    print(f"{'='*80}")

    query2_external = f"""
    SELECT COUNT(DISTINCT PULocationID)
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_external`
    """

    query2_materialized = f"""
    SELECT COUNT(DISTINCT PULocationID)
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    """

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    # External Table
    dry_run_external = client.query(query2_external, job_config=job_config)
    ext_bytes = dry_run_external.total_bytes_processed
    ext_mb = ext_bytes / (1024 * 1024)

    # Materialized Table
    dry_run_materialized = client.query(query2_materialized, job_config=job_config)
    mat_bytes = dry_run_materialized.total_bytes_processed
    mat_mb = mat_bytes / (1024 * 1024)

    print(f"External Table: {ext_mb:.2f} MB ({ext_bytes:,} bytes)")
    print(f"Materialized Table: {mat_mb:.2f} MB ({mat_bytes:,} bytes)")

    # Question 4: fare_amount가 0인 레코드 수
    query4 = f"""
    SELECT COUNT(*) AS zero_fare_count
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    WHERE fare_amount = 0
    """
    run_query_with_stats(4, "Records with fare_amount = 0", query4)

    # Question 5: Partitioned and Clustered Table 생성
    print(f"\n{'='*80}")
    print(f"Question 5: Creating Partitioned and Clustered Table")
    print(f"{'='*80}")

    query5 = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_partitioned_clustered`
    PARTITION BY DATE(tpep_dropoff_datetime)
    CLUSTER BY VendorID AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    """

    try:
        query_job = client.query(query5)
        query_job.result()
        print("✓ Partitioned and clustered table created")
        print("  Strategy: PARTITION BY tpep_dropoff_datetime, CLUSTER BY VendorID")
    except Exception as e:
        print(f"Error: {e}")

    # Question 6: Partitioned vs Non-Partitioned 비교
    print(f"\n{'='*80}")
    print(f"Question 6: Partitioned vs Non-Partitioned Table Comparison")
    print(f"{'='*80}")

    query6_non_partitioned = f"""
    SELECT DISTINCT VendorID
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
    """

    query6_partitioned = f"""
    SELECT DISTINCT VendorID
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_partitioned_clustered`
    WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
    """

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    # Non-partitioned
    dry_run_non_part = client.query(query6_non_partitioned, job_config=job_config)
    non_part_bytes = dry_run_non_part.total_bytes_processed
    non_part_mb = non_part_bytes / (1024 * 1024)

    # Partitioned
    dry_run_part = client.query(query6_partitioned, job_config=job_config)
    part_bytes = dry_run_part.total_bytes_processed
    part_mb = part_bytes / (1024 * 1024)

    print(f"Non-partitioned table: {non_part_mb:.2f} MB ({non_part_bytes:,} bytes)")
    print(f"Partitioned table: {part_mb:.2f} MB ({part_bytes:,} bytes)")

    # Question 9: COUNT(*) 예상 바이트
    query9 = f"""
    SELECT COUNT(*)
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized`
    """
    run_query_with_stats(9, "COUNT(*) from materialized table", query9)

    print(f"\n{'='*80}")
    print("All queries completed!")
    print(f"{'='*80}")
