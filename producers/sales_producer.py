# sales_producer.py
import sqlite3
import requests
import time
from datetime import datetime

DB_PATH = "sales_data.sqlite"
API_URL = "https://www.instacart.com/store/target/storefront?ksadid=4572453&kskwid=1143375&msclkid=f1c6fda7fe311e8d71149a116bf5c254&utm_campaign=ad_demand_search_partner_target_exact_us_AUDACT&utm_content=accountid-47002375_campaignid-367844907_adgroupid-1218259347751930_device-c_adid-76141343049391_network-o&utm_medium=sem&utm_source=instacart_bing&utm_term=matchtype-e_keyword-target_targetid-kwd-76141551041481%3Aloc-190_locationid-49801"  # Replace with real API endpoint

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
        time.sleep(15)  # Fetch new data every 5 seconds
