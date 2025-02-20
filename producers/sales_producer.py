# sales_producer.py
import sqlite3
import requests
import time
from datetime import datetime

DB_PATH = "sales_data.sqlite"
API_URL = "https://fakestoreapi.com/carts?limit=5"  # Replace with real API endpoint

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

if __name__ == "__main__":
    while True:
        fetch_live_sales()
        time.sleep(5)  # Fetch new data every 5 seconds
