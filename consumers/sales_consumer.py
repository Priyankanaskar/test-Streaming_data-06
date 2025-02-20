# sales_consumer.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
from matplotlib.animation import FuncAnimation

DB_PATH = "sales_data.sqlite"
conn = sqlite3.connect(DB_PATH)

def animate(i):
    df = pd.read_sql_query("SELECT * FROM sales_transactions", conn)
    if df.empty:
        print("⚠️ Not enough data for visualization.")
        return

    plt.clf()
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.patch.set_facecolor('#f5f5f5')  # Set background color
    
    df_heatmap = df.groupby(["product_category", "payment_method"]).size().unstack(fill_value=0)
    sns.heatmap(df_heatmap, cmap="coolwarm", annot=True, fmt=".0f", ax=axes[0, 0])
    axes[0, 0].set_title("Sales Heatmap: Product vs Payment", backgroundcolor='#f0f0f0')
    
    df_bar = df.groupby("product_category").size().reset_index(name="sales_count")
    sns.barplot(x="product_category", y="sales_count", data=df_bar, palette="viridis", ax=axes[0, 1])
    axes[0, 1].set_title("Sales Count by Product Category", backgroundcolor='#f0f0f0')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    axes[0, 2].pie(df_bar["sales_count"], labels=df_bar["product_category"], autopct='%1.1f%%', colors=sns.color_palette("pastel"))
    axes[0, 2].set_title("Sales Proportion by Product Category", backgroundcolor='#f0f0f0')
    
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df_line = df.set_index("timestamp").resample("1T").size()
    df_line.plot(ax=axes[1, 0], marker='o', linestyle='-')
    axes[1, 0].set_title("Sales Trend Over Time", backgroundcolor='#f0f0f0')
    
    sns.histplot(df["price"], bins=20, kde=True, ax=axes[1, 1])
    axes[1, 1].set_title("Sales Price Distribution", backgroundcolor='#f0f0f0')
    
    sns.boxplot(x="product_category", y="price", data=df, palette="Set3", ax=axes[1, 2])
    axes[1, 2].set_title("Price Variation by Product Category", backgroundcolor='#f0f0f0')
    axes[1, 2].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    ani = FuncAnimation(plt.gcf(), animate, interval=5000)
    plt.show()
