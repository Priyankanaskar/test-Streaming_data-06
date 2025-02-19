from kafka import KafkaConsumer
import sqlite3
import json

# SQLite Database Setup
conn = sqlite3.connect("chat.db")
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS chat (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT,
    message TEXT
)
""")
conn.commit()

# Kafka Consumer Setup
consumer = KafkaConsumer(
    "chat_stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# Consume messages and store them in SQLite
for message in consumer:
    chat_data = message.value
    cursor.execute("INSERT INTO chat (user, message) VALUES (?, ?)",
                   (chat_data["user"], chat_data["message"]))
    conn.commit()
    print(f"Stored: {chat_data}")
