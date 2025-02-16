import matplotlib.pyplot as plt
import sqlite3
from datetime import datetime
import utils.utils_config as config
from utils.utils_logger import logger


def fetch_keyword_data(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='streamed_messages'")
    if cursor.fetchone() is None:
        logger.error("The 'streamed_messages' table does not exist in the database.")
        conn.close()
        return []

    query = """
    SELECT timestamp, keyword_mentioned
    FROM streamed_messages
    WHERE keyword_mentioned IS NOT NULL
    ORDER BY timestamp
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return data


def plot_keyword_charts(data):
    keywords = {}
    timestamps = []
    keyword_counts = {}

    for timestamp_str, keyword in data:
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        timestamps.append(timestamp)

        if keyword not in keywords:
            keywords[keyword] = [0] * len(timestamps)
            keyword_counts[keyword] = 0
        else:
            keywords[keyword].extend([0] * (len(timestamps) - len(keywords[keyword])))

        keywords[keyword][-1] += 1
        keyword_counts[keyword] += 1  # Track total frequency

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), dpi=100, constrained_layout=True)

    # === Plot 1: Cumulative Keyword Mentions Over Time === #
    for keyword, counts in keywords.items():
        cumulative_counts = [sum(counts[:i + 1]) for i in range(len(counts))]
        min_length = min(len(timestamps), len(cumulative_counts))
        axes[0].plot(timestamps[:min_length], cumulative_counts[:min_length], label=keyword, marker='o', linestyle='-', linewidth=2)

    axes[0].set_title("Cumulative Keyword Mentions Over Time", fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Timestamp", fontsize=10)
    axes[0].set_ylabel("Cumulative Count", fontsize=10)
    axes[0].legend(fontsize=9, loc="upper left", frameon=True)
    axes[0].grid(True, linestyle="--", alpha=0.6)
    axes[0].tick_params(axis='x', rotation=30)

    # === Plot 2: Keyword Frequency (Bar Chart) === #
    bars = axes[1].bar(keyword_counts.keys(), keyword_counts.values(), color=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99'], edgecolor='black')
    
    axes[1].set_title("Keyword Frequency", fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Keywords", fontsize=10)
    axes[1].set_ylabel("Frequency", fontsize=10)
    axes[1].grid(axis="y", linestyle="--", alpha=0.6)
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        axes[1].text(bar.get_x() + bar.get_width()/2, height, f'{height}', ha='center', va='bottom', fontsize=9, fontweight="bold")

    # === Plot 3: Message Timestamp Distribution (Histogram) === #
    axes[2].hist(timestamps, bins=10, color='#ffcc99', edgecolor='black', alpha=0.75)
    axes[2].set_title("Message Timestamp Distribution", fontsize=12, fontweight="bold")
    axes[2].set_xlabel("Timestamp", fontsize=10)
    axes[2].set_ylabel("Message Count", fontsize=10)
    axes[2].grid(True, linestyle="--", alpha=0.6)
    axes[2].tick_params(axis='x', rotation=30)

    plt.show()


def main():
    logger.info("Starting Keyword Tracker")

    try:
        sqlite_path = config.get_sqlite_path()
        logger.info(f"Using SQLite database at: {sqlite_path}")

        data = fetch_keyword_data(sqlite_path)
        logger.info(f"Fetched {len(data)} data points")

        if data:
            plot_keyword_charts(data)
            logger.info("Successfully plotted keyword charts")
        else:
            logger.warning("No keyword data found in the database")

    except Exception as e:
        logger.error(f"Error in Keyword Tracker: {e}")


if __name__ == "__main__":
    main()
