from kafka import KafkaConsumer
import json

# Kafka Consumer Setup
consumer = KafkaConsumer(
    "email_stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# Processing messages from Kafka
for message in consumer:
    email_data = message.value
    print(f"Consumed: {email_data}")
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime

# Initialize empty DataFrame
df = pd.DataFrame(columns=["timestamp", "sender", "recipient", "subject", "body"])

# Processing messages and storing them
for message in consumer:
    email_data = message.value
    email_data["timestamp"] = datetime.fromtimestamp(email_data["timestamp"])
    df = pd.concat([df, pd.DataFrame([email_data])], ignore_index=True)

    # Plot graphs every 10 messages
    if len(df) % 10 == 0:
        print(df.tail(5))  # Print last 5 messages
plt.figure(figsize=(12, 6))
df["timestamp"] = pd.to_datetime(df["timestamp"])
df.set_index("timestamp", inplace=True)
df["count"] = 1
df.resample("1min")["count"].sum().plot(kind="line", marker="o", color="b")
plt.title("Number of Emails Over Time")
plt.xlabel("Time")
plt.ylabel("Email Count")
plt.show()
plt.figure(figsize=(12, 6))
top_senders = df["sender"].value_counts().head(5)
sns.barplot(x=top_senders.index, y=top_senders.values)
plt.xticks(rotation=45)
plt.title("Top 5 Email Senders")
plt.ylabel("Count")
plt.show()
plt.figure(figsize=(12, 6))
hourly_counts = df.groupby(df.index.hour)["count"].sum()
sns.heatmap(hourly_counts.values.reshape(-1, 1), cmap="coolwarm", annot=True)
plt.title("Email Activity by Hour")
plt.xlabel("Hour")
plt.show()
