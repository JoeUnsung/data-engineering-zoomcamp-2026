"""Q2: Send green taxi data to Redpanda green-trips topic"""

import json
import dataclasses
from time import time

import pandas as pd
from kafka import KafkaProducer

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from models import GreenRide, green_ride_from_row


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def main():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
    columns = [
        'lpep_pickup_datetime', 'lpep_dropoff_datetime',
        'PULocationID', 'DOLocationID',
        'passenger_count', 'trip_distance',
        'tip_amount', 'total_amount',
    ]
    df = pd.read_parquet(url, columns=columns)
    print(f"Total rows: {len(df)}")

    server = 'localhost:9092'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer,
    )

    topic_name = 'green-trips'

    t0 = time()

    for _, row in df.iterrows():
        ride = green_ride_from_row(row)
        producer.send(topic_name, value=dataclasses.asdict(ride))

    producer.flush()

    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')


if __name__ == '__main__':
    main()
