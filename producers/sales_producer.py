import sqlite3
import random
import time
from datetime import datetime

DB_PATH = "sales_data.sqlite"
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Toys", "Sports"]
PAYMENT_METHODS = ["Credit Card", "PayPal", "Cryptocurrency", "Cash"]

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

def generate_and_store_transaction():
    transaction = {
        "timestamp": datetime.utcnow().isoformat(),
        "product_category": random.choice(CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "price": round(random.uniform(5.0, 500.0), 2)
    }

    cursor.execute('''
        INSERT INTO sales_transactions (timestamp, product_category, payment_method, price)
        VALUES (?, ?, ?, ?)
    ''', (transaction["timestamp"], transaction["product_category"], transaction["payment_method"], transaction["price"]))
    
    conn.commit()
    print(f"🛍️ New Sale Inserted: {transaction}")

if __name__ == "__main__":
    while True:
        generate_and_store_transaction()
        time.sleep(2)
