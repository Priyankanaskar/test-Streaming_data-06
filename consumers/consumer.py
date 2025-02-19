import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import defaultdict

# Track sales by category
sales_data = defaultdict(float)

def read_latest_transactions():
    """Read the last 20 transactions from the file."""
    try:
        with open("data/transactions.json", "r") as f:
            lines = f.readlines()[-20:]  # Get the last 20 transactions
        return [json.loads(line) for line in lines]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def update_chart(frame):
    """Update the bar chart with the latest sales data."""
    transactions = read_latest_transactions()
    
    # Reset sales data
    global sales_data
    sales_data.clear()
    
    for txn in transactions:
        sales_data[txn["product_category"]] += txn["purchase_amount"]

    # Clear the plot
    ax.clear()
    ax.bar(sales_data.keys(), sales_data.values(), color="skyblue")
    ax.set_title("Real-Time Sales by Category")
    ax.set_xlabel("Product Category")
    ax.set_ylabel("Total Sales ($)")
    ax.set_ylim(0, max(sales_data.values(), default=100) + 50)

# Initialize Matplotlib figure
fig, ax = plt.subplots()
ani = animation.FuncAnimation(fig, update_chart, interval=3000)  # Update every 3 seconds

plt.show()