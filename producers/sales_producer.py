# sales_producer.py
import sqlite3
import requests
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.animation import FuncAnimation

DB_PATH = "sales_data.sqlite"
API_URL = "https://api.sampleapis.com/fakestore/products"  # Replace with real API (Walmart, Amazon, etc.)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_category TEXT,
        payment_method TEXT,
        price REAL
    )
''')
conn.commit()

def fetch_live_sales():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            sales_data = response.json()
            for sale in sales_data:
                timestamp = datetime.utcnow().isoformat()
                product_category = sale.get("category", "Unknown")
                payment_method = "Credit Card"  # Default, modify as needed
                price = float(sale.get("price", 0))

                cursor.execute('''
                    INSERT INTO sales_transactions (timestamp, product_category, payment_method, price)
                    VALUES (?, ?, ?, ?)
                ''', (timestamp, product_category, payment_method, price))
                conn.commit()
                print(f"🛍️ New Sale Inserted: {timestamp}, {product_category}, {payment_method}, {price}")
    except Exception as e:
        print(f"Error fetching sales data: {e}")

def animate(i):
    df = pd.read_sql_query("SELECT * FROM sales_transactions", conn)
    if df.empty:
        print("⚠️ Not enough data for visualization.")
        return

    plt.clf()
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.patch.set_facecolor('#f5f5f5')  # Set background color
    
    df_bar = df.groupby("product_category").size().reset_index(name="sales_count")
    sns.barplot(x="product_category", y="sales_count", data=df_bar, palette="viridis", ax=axes[0])
    axes[0].set_title("Sales Count by Product Category", backgroundcolor='#f0f0f0')
    axes[0].tick_params(axis='x', rotation=45)
    
    axes[1].pie(df_bar["sales_count"], labels=df_bar["product_category"], autopct='%1.1f%%', colors=sns.color_palette("pastel"))
    axes[1].set_title("Sales Proportion by Product Category", backgroundcolor='#f0f0f0')
    
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df_line = df.set_index("timestamp").resample("1T").size()
    df_line.plot(ax=axes[2], marker='o', linestyle='-')
    axes[2].set_title("Sales Trend Over Time", backgroundcolor='#f0f0f0')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    ani = FuncAnimation(plt.gcf(), animate, interval=10000)
    while True:
        fetch_live_sales()
        time.sleep(5)
