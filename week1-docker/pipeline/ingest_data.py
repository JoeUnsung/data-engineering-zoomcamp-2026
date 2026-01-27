#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'

    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()
# #!/usr/bin/env python
# # coding: utf-8
# import pandas as pd
# from sqlalchemy import create_engine
# from tqdm.auto import tqdm

# year = 2021
# month = 1

# # 데이터 딕셔너리
# # https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf




# dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }


# # In[18]:


# parse_dates = [
#     "tpep_pickup_datetime",
#     "tpep_dropoff_datetime"
# ]

# df = pd.read_csv(
#     url,
#     # nrows=100,
#     dtype=dtype,
#     parse_dates=parse_dates
# )

# pg_user = 'root'
# pg_pass = 'root'
# pg_host = 'localhost'
# pg_port = 5432
# pg_db = 'ny_taxi'


# # CREATE TABLE yellow_taxi_data (
# # 	"VendorID" BIGINT, 
# # 	tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
# # 	tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
# # 	passenger_count BIGINT, 
# # 	trip_distance FLOAT(53), 
# # 	"RatecodeID" BIGINT, 
# # 	store_and_fwd_flag TEXT, 
# # 	"PULocationID" BIGINT, 
# # 	"DOLocationID" BIGINT, 
# # 	payment_type BIGINT, 
# # 	fare_amount FLOAT(53), 
# # 	extra FLOAT(53), 
# # 	mta_tax FLOAT(53), 
# # 	tip_amount FLOAT(53), 
# # 	tolls_amount FLOAT(53), 
# # 	improvement_surcharge FLOAT(53), 
# # 	total_amount FLOAT(53), 
# # 	congestion_surcharge FLOAT(53)
# # )

# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# df_iter = pd.read_csv(
#     url,
#     # nrows=100,
#     dtype=dtype,
#     parse_dates=parse_dates,
#     iterator=True,
#     chunksize=100000
# )




# df_iter = pd.read_csv(
#     url,
#     # nrows=100,
#     dtype=dtype,
#     parse_dates=parse_dates,
#     iterator=True,
#     chunksize=100000
# )


# for df_chunk in tqdm(df_iter):
#     df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')




