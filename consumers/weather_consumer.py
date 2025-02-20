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
conn = sqlite3.connect(DB_NAME, check_same_thread=False)
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

# Enable interactive mode for live updates
plt.ion()

while True:
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

            # Clear previous plots
            plt.clf()

            # Create a figure with a 1x3 layout
            fig, axes = plt.subplots(1, 3, figsize=(15, 5))
            fig.suptitle("📊 Live Weather Data Visualization")

            # 🔹 **1️⃣ Line Chart - Temperature Over Time**
            axes[0].plot(df["timestamp"], df["temperature"], marker="o", linestyle="-", color="blue")
            axes[0].set_xlabel("Time")
            axes[0].set_ylabel("Temperature (°C)")
            axes[0].set_title("📈 Temperature Over Time")
            axes[0].tick_params(axis="x", rotation=45)
            axes[0].grid()

            # 🔹 **2️⃣ Bar Chart - Wind Speed Over Time**
            axes[1].bar(df["timestamp"], df["windspeed"], color="green")
            axes[1].set_xlabel("Time")
            axes[1].set_ylabel("Wind Speed (km/h)")
            axes[1].set_title("🌬️ Wind Speed Over Time")
            axes[1].tick_params(axis="x", rotation=45)
            axes[1].grid(axis="y")

            # 🔹 **3️⃣ Pie Chart - Weather Code Distribution**
            weather_counts = df["weather_code"].value_counts()
            labels = [f"Code {code}" for code in weather_counts.index]
            axes[2].pie(weather_counts, labels=labels, autopct="%1.1f%%", colors=["red", "blue", "orange", "green"])
            axes[2].set_title("☁️ Weather Condition Distribution")

            # Adjust layout and update the plot
            plt.tight_layout()
            plt.draw()
            plt.pause(2)  # Refresh every 2 seconds

