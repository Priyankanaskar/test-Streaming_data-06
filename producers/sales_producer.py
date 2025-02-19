import sqlite3
import random
import time
from datetime import datetime

# Define SQLite database path
DB_PATH = "sales_data.sqlite"

# Define product categories and payment methods
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Toys", "Sports"]
PAYMENT_METHODS = ["Credit Card", "PayPal", "Cryptocurrency"]

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Ensure the sales table exists
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

# Function to generate and insert a fake transaction
def generate_and_store_transaction():
    transaction = {
        "timestamp": datetime.utcnow().isoformat(),
        "product_category": random.choice(CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "price": round(random.uniform(5.0, 500.0), 2)
    }

    # Insert transaction into SQLite
    cursor.execute('''
        INSERT INTO sales_transactions (timestamp, product_category, payment_method, price)
        VALUES (?, ?, ?, ?)
    ''', (transaction["timestamp"], transaction["product_category"], transaction["payment_method"], transaction["price"]))
    
    conn.commit()
    print(f"🛍️ New Sale Inserted: {transaction}")

# Continuously generate transactions
if __name__ == "__main__":
    while True:
        generate_and_store_transaction()
        time.sleep(2)  # Generate a new sale every 2 seconds