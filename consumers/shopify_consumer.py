import matplotlib.pyplot as plt
import pandas as pd
import threading
from kafka import KafkaConsumer
import json
import matplotlib.animation as animation

# Configure Kafka Consumer
consumer = KafkaConsumer(
    'shopify_sales_data',
    bootstrap_servers=['localhost:9092'],
    group_id='shopify-sales-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize an empty DataFrame to store sales data
sales_df = pd.DataFrame(columns=['timestamp', 'total_price', 'order_id', 'product', 'location'])

# Function to update the charts
def update_sales_data(frame):
    global sales_df
    
    # Limit to the latest 50 sales data points for visualization
    sales_df = sales_df.tail(50)
    
    # Clear previous plots
    for ax in axes:
        ax.clear()
    
    # Total sales over time (Chart 1)
    axes[0].plot(pd.to_datetime(sales_df['timestamp']), sales_df['total_price'], marker='o', color='b', label='Total Sales')
    axes[0].set_title('Total Sales Over Time')
    axes[0].set_xlabel('Time')
    axes[0].set_ylabel('Total Sales ($)')
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].set_facecolor('lightyellow')
    
    # Number of orders over time (Chart 2)
    axes[1].plot(pd.to_datetime(sales_df['timestamp']), sales_df['order_id'], marker='o', color='g', label='Orders')
    axes[1].set_title('Number of Orders Over Time')
    axes[1].set_xlabel('Time')
    axes[1].set_ylabel('Number of Orders')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].set_facecolor('lightcyan')

    # Sales distribution by product (Chart 3)
    product_sales = sales_df['product'].value_counts()
    axes[2].bar(product_sales.index, product_sales.values, color='purple')
    axes[2].set_title('Sales Distribution by Product')
    axes[2].set_xlabel('Product')
    axes[2].set_ylabel('Sales Count')
    axes[2].set_facecolor('lightgreen')

    # Sales by location (Chart 4)
    location_sales = sales_df['location'].value_counts()
    axes[3].bar(location_sales.index, location_sales.values, color='orange')
    axes[3].set_title('Sales by Location')
    axes[3].set_xlabel('Location')
    axes[3].set_ylabel('Sales Count')
    axes[3].set_facecolor('lightcoral')

    plt.tight_layout()

# Function to consume messages from Kafka
def consume_sales_data():
    global sales_df
    for message in consumer:
        sale = message.value
        # Append the new sales data to the DataFrame
        new_row = {
            'timestamp': sale['created_at'],
            'total_price': sale['total_price'],
            'order_id': sale['order_id'],
            'product': sale['line_items'][0]['name'] if sale['line_items'] else 'Unknown Product',
            'location': sale['location'] if 'location' in sale else 'Unknown Location'
        }
        sales_df = sales_df.append(new_row, ignore_index=True)

# Set up the plot with 4 subplots (4 charts)
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Start a separate thread for consuming Kafka data
consumer_thread = threading.Thread(target=consume_sales_data, daemon=True)
consumer_thread.start()

# Set up real-time updates with animation
ani = animation.FuncAnimation(fig, update_sales_data, interval=5000)  # Update every 5 seconds

# Show the plot
plt.show()
