import sqlite3
import json
from kafka import KafkaConsumer

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'weather_data',
    bootstrap_servers=['localhost:9092'],
    group_id='weather-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# SQLite Database Configuration
DB_NAME = 'weather_data.db'

# Create SQLite Table
def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            latitude REAL,
            longitude REAL,
            temperature REAL,
            windspeed REAL,
            weather_code INTEGER,
            timestamp TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Insert Weather Data into SQLite
def insert_weather_data(data):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO weather (latitude, longitude, temperature, windspeed, weather_code, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data['latitude'], data['longitude'], data['temperature'], data['windspeed'], data['weather_code'], data['timestamp']))
        conn.commit()
        conn.close()
        print(f"Data stored successfully: {data}")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Consume Kafka Messages and Store in SQLite
def consume_weather_data():
    create_table()
    print("Waiting for weather data from Kafka...")
    for message in consumer:
        weather_data = message.value
        print(f"Received: {weather_data}")
        insert_weather_data(weather_data)

if __name__ == '__main__':
    consume_weather_data()
