import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import random
import time

#  Define SQLite database path
DB_PATH = "sales_data.sqlite"

#  Connect to SQLite Database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

#  Create sales_transactions table if it doesn’t exist
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

#  Function to Insert Simulated Sales Data into SQLite
def generate_fake_sale():
    categories = ["Electronics", "Clothing", "Home Goods", "Books", "Toys"]
    payment_methods = ["Credit Card", "PayPal", "Cash"]
    
    transaction = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "product_category": random.choice(categories),
        "payment_method": random.choice(payment_methods),
        "price": round(random.uniform(5, 500), 2)
    }

    cursor.execute('''
        INSERT INTO sales_transactions (timestamp, product_category, payment_method, price)
        VALUES (?, ?, ?, ?)
    ''', (transaction["timestamp"], transaction["product_category"], transaction["payment_method"], transaction["price"]))
    conn.commit()
    
    print(f"🛒 New Sale: {transaction}")

#  Function to Generate Charts
def plot_all_charts():
    # Query Data
    heatmap_query = "SELECT product_category, payment_method, COUNT(*) as sales_count FROM sales_transactions GROUP BY product_category, payment_method"
    bar_pie_query = "SELECT product_category, COUNT(*) as sales_count FROM sales_transactions GROUP BY product_category"

    df_heatmap = pd.read_sql_query(heatmap_query, conn)
    df_bar_pie = pd.read_sql_query(bar_pie_query, conn)

    # If there is no data, return early
    if df_heatmap.empty or df_bar_pie.empty:
        print("⚠️ Not enough data for visualization.")
        return

    # 📊 Create Figure with 3 Subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 🔥 Heatmap: Sales by Product & Payment Method
    heatmap_data = df_heatmap.pivot(index="product_category", columns="payment_method", values="sales_count").fillna(0)
    sns.heatmap(heatmap_data, cmap="coolwarm", annot=True, fmt=".0f", ax=axes[0])
    axes[0].set_title("Sales Heatmap: Product vs Payment")
    axes[0].set_xlabel("Payment Method")
    axes[0].set_ylabel("Product Category")

    # 📊 Bar Chart: Sales Count by Product Category
    sns.barplot(x="product_category", y="sales_count", data=df_bar_pie, palette="viridis", ax=axes[1])
    axes[1].set_title("Sales Count by Product Category")
    axes[1].set_xlabel("Product Category")
    axes[1].set_ylabel("Sales Count")
    axes[1].tick_params(axis='x', rotation=45)

    # 🥧 Pie Chart: Sales Proportion by Product Category
    axes[2].pie(df_bar_pie["sales_count"], labels=df_bar_pie["product_category"], autopct='%1.1f%%', colors=sns.color_palette("pastel"))
    axes[2].set_title("Sales Proportion by Product Category")

    # Adjust Layout & Show All Charts at Once
    plt.tight_layout()
    plt.show()

#  Run Simulation & Update Charts
while True:
    generate_fake_sale()  # Insert a new sales record
    print("✅ Sale stored in SQLite!")

    # 📊 Show updated charts
    plot_all_charts()

    time.sleep(3)  # Simulate new sales every 3 seconds


