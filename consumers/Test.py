import sqlite3

# Connect to SQLite database
conn = sqlite3.connect("chat.db")
cursor = conn.cursor()

# Fetch all stored chat messages
cursor.execute("SELECT * FROM chat")
rows = cursor.fetchall()

# Print each message
for row in rows:
    print(f"{row[1]}: {row[2]}")  # row[1] = user, row[2] = message

conn.close()
