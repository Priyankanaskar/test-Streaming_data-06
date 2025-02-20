import matplotlib.pyplot as plt
import pandas as pd
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

# Initialize an empty list to store data
sales_df = pd.DataFrame(columns=['timestamp', 'total_price', 'order_id'])

# Function to update the chart in real-time
def update_sales_data(frame):
    global sales_df
    
    # Consume the latest message from Kafka
    for message in consumer:
        sale = message.value
        
        # Add the new sales data to the DataFrame
        new_row = {
            'timestamp': sale['created_at'],
            'total_price': sale['total_price'],
            'order_id': sale['order_id']
        }
        
        sales_df = sales_df.append(new_row, ignore_index=True)
        
        # Limit to the latest 50 sales data points for visualization
        sales_df = sales_df.tail(50)
        
        # Clear previous plot
        plt.clf()

        # Plot real-time sales data
        plt.plot(pd.to_datetime(sales_df['timestamp']), sales_df['total_price'], marker='o', color='b', label='Total Sales')
        plt.title('Real-Time Sales Data')
        plt.xlabel('Time')
        plt.ylabel('Total Sales ($)')
        plt.xticks(rotation=45)
        plt.tight_layout()

# Set up the plot for real-time updates
fig = plt.figure(figsize=(10, 6))
ani = animation.FuncAnimation(fig, update_sales_data, interval=5000)  # Update every 5 seconds

plt.show()
