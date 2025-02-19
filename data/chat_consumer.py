import sqlite3

conn = sqlite3.connect("chat.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM chat")
rows = cursor.fetchall()

for row in rows:
    print(f"{row[1]}: {row[2]}")
