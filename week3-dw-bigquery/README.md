# Week 3: Data Warehousing with BigQuery

This week focuses on data warehousing concepts using Google BigQuery, including external tables, partitioning, clustering, and query optimization.

---

## Setup

### Prerequisites
- Google Cloud Platform (GCP) account
- GCS bucket created in `us-west1` region
- Service account credentials (`gcs.json`)

### Data Upload to GCS

**Upload Yellow Taxi 2024 data (Jan-Jun) to GCS:**
```bash
cd week3-dw-bigquery
pip install google-cloud-storage requests
python3 load_yellow_taxi_data.py
```

This script will:
1. Download Yellow Taxi parquet files for 2024 (Jan-Jun)
2. Upload them to the GCS bucket: `data-engineering-zoomcamp-2026`
3. Verify all uploads

**Files uploaded:**
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-01.parquet`
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-02.parquet`
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-03.parquet`
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-04.parquet`
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-05.parquet`
- `gs://data-engineering-zoomcamp-2026/yellow_tripdata_2024-06.parquet`

### BigQuery Setup

**Run homework queries:**
```bash
pip install google-cloud-bigquery
python3 run_homework_queries.py
```

This will:
1. Create the `taxi_data` dataset in BigQuery
2. Create an external table pointing to GCS parquet files
3. Create a materialized table
4. Execute all homework queries and display results

---

## Homework

### Question 1. What is count of records for the 2024 Yellow Taxi Data? (1 point)

**Options:**
- 65,623
- 840,402
- 20,332,093
- 85,431,289

**SQL Query:**
```sql
SELECT COUNT(*) AS record_count
FROM `taxi_data.yellow_taxi_materialized`;
```

**Answer:** 20,332,093

**Key Learning:**
- BigQuery can efficiently count records using metadata
- COUNT(*) operations on materialized tables don't scan actual data (0 bytes processed)

---

### Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? (1 point)

**Options:**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

**SQL Queries:**
```sql
-- External Table
SELECT COUNT(DISTINCT PULocationID)
FROM `taxi_data.yellow_taxi_external`;

-- Materialized Table
SELECT COUNT(DISTINCT PULocationID)
FROM `taxi_data.yellow_taxi_materialized`;
```

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

**Key Learning:**
- External tables don't have size estimation in dry-run mode (shows 0 MB)
- Materialized tables have metadata for accurate byte estimation
- BigQuery scans only the requested columns (columnar storage benefit)

---

### Question 3. Why are the estimated number of Bytes different? (1 point)

**Options:**
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

**Answer:** BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**Key Learning:**
- BigQuery's columnar storage only reads the columns needed for the query
- More columns = more data to scan
- This is a fundamental advantage of columnar databases for analytical workloads

---

### Question 4. How many records have a fare_amount of 0? (1 point)

**Options:**
- 128,210
- 546,578
- 20,188,016
- 8,333

**SQL Query:**
```sql
SELECT COUNT(*) AS zero_fare_count
FROM `taxi_data.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

**Answer:** 8,333

**Key Learning:**
- Data quality issues are common in real-world datasets
- Zero fare amounts could indicate free rides, data errors, or special cases

---

### Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID? (1 point)

**Options:**
- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

**SQL Query:**
```sql
CREATE OR REPLACE TABLE `taxi_data.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `taxi_data.yellow_taxi_materialized`;
```

**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

**Key Learning:**
- **Partitioning**: Best for high-cardinality columns used in WHERE clauses (dates, timestamps)
  - Eliminates entire partitions from scans
  - Reduces query costs significantly
- **Clustering**: Best for low-cardinality columns used for sorting/filtering (VendorID has few values)
  - Physically organizes data within partitions
  - Improves query performance for sorted operations
- You can only partition by one column but cluster by multiple columns (up to 4)

---

### Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? (1 point)

**Options:**
- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

**SQL Queries:**
```sql
-- Non-partitioned table
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_materialized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned table
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

**Key Learning:**
- Partitioning dramatically reduces data scanned (91% reduction in this case!)
- Non-partitioned table must scan the entire table
- Partitioned table only scans the relevant date partitions (15 days of data)
- This translates to significant cost savings on large datasets

---

### Question 7. Where is the data stored in the External Table you created? (1 point)

**Options:**
- Big Query
- Container Registry
- GCP Bucket
- Big Table

**Answer:** GCP Bucket

**Key Learning:**
- External tables don't store data in BigQuery storage
- They point to data in external sources (GCS, Cloud Storage, Drive, etc.)
- Data remains in the original location (GCS bucket in our case)
- External tables are useful for:
  - Querying data without importing it
  - ETL preprocessing
  - Data lake architectures

---

### Question 8. It is best practice in Big Query to always cluster your data: (1 point)

**Options:**
- True
- False

**Answer:** False

**Key Learning:**
- Clustering is NOT always beneficial:
  - **Use clustering when**: You frequently filter/sort by specific columns with low-to-medium cardinality
  - **Don't use clustering when**:
    - Tables are very small (< 1 GB)
    - Query patterns don't benefit from physical ordering
    - Columns have extremely high cardinality
- Over-clustering can increase metadata overhead without performance benefits
- Always analyze query patterns before deciding on clustering strategy

---

### Question 9. Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why? (not graded)

**SQL Query:**
```sql
SELECT COUNT(*)
FROM `taxi_data.yellow_taxi_materialized`;
```

**Answer:** 0 MB

**Why?**
- BigQuery stores row count in table metadata
- COUNT(*) doesn't need to scan actual table data
- The metadata contains:
  - Total number of rows
  - Schema information
  - Partition information (if partitioned)
- This makes COUNT(*) operations extremely fast and free

**Key Learning:**
- BigQuery optimizes COUNT(*) using metadata
- No actual data scanning = no processing costs
- This is different from COUNT(column_name) which may need to scan data to count non-NULL values

---

## Summary

### Technologies Used
- **Google Cloud Storage (GCS)**: Object storage for parquet files
- **Google BigQuery**: Serverless data warehouse
- **Python**: Data upload and query automation
  - `google-cloud-storage`: GCS client library
  - `google-cloud-bigquery`: BigQuery client library
  - `requests`: HTTP library for downloading files
- **Parquet**: Columnar file format

### Key Learnings

1. **External vs Materialized Tables**:
   - External tables: Data stays in GCS, no BigQuery storage costs
   - Materialized tables: Data copied to BigQuery, faster queries, better optimization

2. **Columnar Storage Benefits**:
   - Only scan columns needed for the query
   - Dramatically reduces I/O for analytical queries
   - BigQuery automatically optimizes column-based storage

3. **Partitioning Strategy**:
   - Partition by high-cardinality columns (dates are ideal)
   - Eliminates entire partitions from scans
   - Can reduce costs by 90%+ on filtered queries

4. **Clustering Strategy**:
   - Cluster by low-to-medium cardinality columns used for filtering/sorting
   - Works within partitions to further organize data
   - Up to 4 clustering columns allowed

5. **Cost Optimization**:
   - Use partitioning and clustering for large tables
   - Limit columns selected in queries
   - Use COUNT(*) instead of COUNT(column) when possible
   - Preview query costs with dry-run before execution

6. **BigQuery Best Practices**:
   - Always check estimated bytes before running queries
   - Partition large tables by date/timestamp
   - Use clustering for frequently filtered columns
   - Avoid SELECT * in production queries
   - Use materialized views for frequently accessed query results

### Files in This Directory

- `load_yellow_taxi_data.py`: Script to download and upload taxi data to GCS
- `run_homework_queries.py`: Script to execute all homework queries in BigQuery
- `bq_query.sql`: SQL queries for BigQuery homework
- `HW_WEEK3.txt`: Homework answers summary
- `gcs.json`: GCS service account credentials (gitignored)

---

## Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Partitioning and Clustering Guide](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
