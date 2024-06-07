from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid

consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_keyspace')

def consume_messages():
    for message in consumer:
        data = message.value
        session.execute(
            """
            INSERT INTO weather_table (id, city, temperature, humidity, weather)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (uuid.UUID(data['id']), data['city'], data['temperature'], data['humidity'], data['weather'])
        )
        print(f"Consumed: {data}")

if __name__ == '__main__':
    consume_messages()
