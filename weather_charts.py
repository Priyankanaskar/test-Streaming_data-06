import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to SQLite database
DB_NAME = "weather_data.db"

def fetch_weather_data():
    """Fetches weather data from SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT timestamp, temperature, windspeed, weather_code FROM weather ORDER BY timestamp ASC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def plot_all_charts():
    df = fetch_weather_data()

    if df.empty:
        print("❌ No data found in database. Run the producer and consumer first.")
        return

    # Convert timestamp to readable format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Create a figure with a 2x2 grid
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 🟢 **1️⃣ Line Chart: Temperature Trend Over Time**
    axes[0, 0].plot(df["timestamp"], df["temperature"], marker="o", linestyle="-", color="blue", label="Temperature (°C)")
    axes[0, 0].set_xlabel("Timestamp")
    axes[0, 0].set_ylabel("Temperature (°C)")
    axes[0, 0].set_title("📈 Temperature Trend Over Time")
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 0].legend()
    axes[0, 0].grid()

    # 🟢 **2️⃣ Bar Chart: Wind Speed Comparison**
    axes[0, 1].bar(df["timestamp"], df["windspeed"], color="green", label="Wind Speed (km/h)")
    axes[0, 1].set_xlabel("Timestamp")
    axes[0, 1].set_ylabel("Wind Speed (km/h)")
    axes[0, 1].set_title("🌬️ Wind Speed Over Time")
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 1].legend()
    axes[0, 1].grid(axis="y")

    # 🟢 **3️⃣ Pie Chart: Weather Code Distribution**
    weather_counts = df["weather_code"].value_counts()
    labels = [f"Code {code}" for code in weather_counts.index]
    axes[1, 0].pie(weather_counts, labels=labels, autopct="%1.1f%%", colors=["red", "blue", "orange", "green"])
    axes[1, 0].set_title("☁️ Weather Condition Distribution")

    # 🟢 **4️⃣ Scatter Plot: Temperature vs. Wind Speed**
    axes[1, 1].scatter(df["temperature"], df["windspeed"], c="purple", alpha=0.7)
    axes[1, 1].set_xlabel("Temperature (°C)")
    axes[1, 1].set_ylabel("Wind Speed (km/h)")
    axes[1, 1].set_title("🔥 Temperature vs. Wind Speed")
    axes[1, 1].grid()

    # Adjust layout and show all charts in one view
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    plot_all_charts()
