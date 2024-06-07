import requests
from kafka import KafkaProducer
import json
import uuid
import time

# Function to fetch data from OpenWeatherMap API

# api_key = "9eb8222bfc35c99d643cba85382a8baa"
# city = "Delhi"

def fetch_data_from_api(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to produce messages to Kafka
def produce_messages(producer, topic, api_key, city):
    while True:
        data = fetch_data_from_api(api_key, city)
        if data:
            message = {
                'id': str(uuid.uuid4()),
                'city': city,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'weather': data['weather'][0]['description']
            }
            producer.send(topic, message)
            producer.flush()
            print(f"Produced: {message}")
        time.sleep(300)  # Fetch data every 5 minutes

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    api_key = 'f1f02daadd8b7f52e689aedffdd0f8f2'  # Replace with your OpenWeatherMap API key
    city = 'bengaluru'  # Replace with the city you want to fetch weather data for
    produce_messages(producer, 'weather_topic', api_key, city)
