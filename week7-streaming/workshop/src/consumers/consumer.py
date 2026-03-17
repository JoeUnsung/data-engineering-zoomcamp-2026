"""Q3: Count trips with trip_distance > 5.0"""

from kafka import KafkaConsumer

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from models import green_ride_deserializer


def main():
    server = 'localhost:9092'
    topic_name = 'green-trips'

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[server],
        auto_offset_reset='earliest',
        group_id='green-trips-counter',
        value_deserializer=green_ride_deserializer,
        consumer_timeout_ms=10000,
    )

    print(f"Listening to {topic_name}...")

    count = 0
    total = 0
    for message in consumer:
        ride = message.value
        total += 1
        if ride.trip_distance > 5.0:
            count += 1
        if total % 10000 == 0:
            print(f"Processed {total} messages, {count} with distance > 5.0")

    print(f"\nTotal messages: {total}")
    print(f"Trips with distance > 5.0: {count}")

    consumer.close()


if __name__ == '__main__':
    main()
