from kafka import KafkaProducer
import json
import time

from consumers.shopify_Api import get_shopify_orders

# Configure your Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Fetch Shopify sales data
def get_shopify_sales_data():
    # Call Shopify API to get orders
    sales_data = get_shopify_orders()
    
    # Send each sale to Kafka topic 'shopify_sales_data'
    for sale in sales_data:
        producer.send('shopify_sales_data', sale)
        print(f"Sent sales data: {sale}")

# Send data every 10 seconds (or adjust the time interval as necessary)
while True:
    get_shopify_sales_data()
    time.sleep(10)
