import requests
import json
import time
from kafka import KafkaProducer

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Open-Meteo API - Fetch Weather Data
def fetch_weather_data(latitude, longitude):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    
    try:
        response = requests.get(url, timeout=10)  # Added timeout
        if response.status_code == 200:
            data = response.json()["current_weather"]
            return {
                'latitude': latitude,
                'longitude': longitude,
                'temperature': data['temperature'],
                'windspeed': data['windspeed'],
                'weather_code': data['weathercode'],
                'timestamp': data['time']
            }
        else:
            print(f"API Error: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# Send Data to Kafka
def produce_weather_data():
    latitude, longitude = 40.7128, -74.0060  # New York
    while True:
        weather_data = fetch_weather_data(latitude, longitude)
        if weather_data:
            producer.send("weather_data", value=weather_data)
            print(f"Sent to Kafka: {weather_data}")
        time.sleep(60)  # Fetch every 60 seconds

if __name__ == '__main__':
    produce_weather_data()
