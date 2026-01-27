#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

def ingest_parquet_to_db(
    file_path: str,
    engine,
    target_table: str,
    chunksize: int = 100000,
):
    """Ingest parquet file to database table"""
    print(f"Reading parquet file: {file_path}")
    
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    print(f"Total records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Create table with first chunk (empty)
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False
    )
    print(f"Table {target_table} created")
    
    # Insert data in chunks
    for i in tqdm(range(0, len(df), chunksize)):
        chunk = df[i:i+chunksize]
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted chunk: {len(chunk)} records")
    
    print(f"Done ingesting {len(df)} records to {target_table}")

if __name__ == "__main__":
    # Database connection parameters
    pg_user = 'root'
    pg_pass = 'root' 
    pg_host = 'localhost'
    pg_port = '5432'
    pg_db = 'ny_taxi'
    
    # Create database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # File path and target table
    file_path = '/Users/joeunsung/git/data-engineering-zoomcamp-2026/week1-docker/pipeline/green_tripdata_2025-11.parquet'
    target_table = 'green_taxi_trips'
    
    # Ingest data
    ingest_parquet_to_db(
        file_path=file_path,
        engine=engine,
        target_table=target_table,
        chunksize=100000
    )