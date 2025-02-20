import sqlite3
import json
import matplotlib.pyplot as plt
import pandas as pd
from kafka import KafkaConsumer

# Kafka settings
KAFKA_TOPIC = "weather_topic"
KAFKA_SERVER = "localhost:9092"

# SQLite settings
DB_NAME = "weather_data.db"

# Connect to SQLite and create table if not exists
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        timestamp TEXT,
        temperature REAL,
        windspeed REAL,
        weather_code INTEGER
    )
""")
conn.commit()

# Kafka Consumer
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda x: json.loads(x.decode("utf-8")))

print("✅ Kafka Consumer Started. Listening for weather data...")

for message in consumer:
    data = message.value

    timestamp = data.get("timestamp")
    temperature = data.get("temperature")
    windspeed = data.get("windspeed")
    weather_code = data.get("weather_code")

    # Store in SQLite
    try:
        cursor.execute("INSERT INTO weather (timestamp, temperature, windspeed, weather_code) VALUES (?, ?, ?, ?)",
                       (timestamp, temperature, windspeed, weather_code))
        conn.commit()
        print(f"📩 Data Stored: {data}")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

    # Fetch latest data for visualization
    df = pd.read_sql_query("SELECT * FROM weather ORDER BY timestamp ASC", conn)

    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # 🔹 **1️⃣ Line Chart - Temperature Over Time**
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 3, 1)  # (rows, cols, position)
        plt.plot(df["timestamp"], df["temperature"], marker="o", linestyle="-", color="blue")
        plt.xlabel("Time")
        plt.ylabel("Temperature (°C)")
        plt.title("📈 Temperature Over Time")
        plt.xticks(rotation=45)
        plt.grid()

        # 🔹 **2️⃣ Bar Chart - Wind Speed Over Time**
        plt.subplot(1, 3, 2)
        plt.bar(df["timestamp"], df["windspeed"], color="green")
        plt.xlabel("Time")
        plt.ylabel("Wind Speed (km/h)")
        plt.title("🌬️ Wind Speed Over Time")
        plt.xticks(rotation=45)
        plt.grid(axis="y")

        # 🔹 **3️⃣ Pie Chart - Weather Code Distribution**
        plt.subplot(1, 3, 3)
        weather_counts = df["weather_code"].value_counts()
        labels = [f"Code {code}" for code in weather_counts.index]
        plt.pie(weather_counts, labels=labels, autopct="%1.1f%%", colors=["red", "blue", "orange", "green"])
        plt.title("☁️ Weather Condition Distribution")

        # Show all charts in one window
        plt.tight_layout()
        plt.pause(5)  # Update every 5 seconds

plt.show()
