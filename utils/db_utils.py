import sqlite3

DB_PATH = "data/sales_data.sqlite"

def get_db_connection():
    """Establish a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_sales_table():
    """Ensure the sales table exists in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
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
    conn.close()

# Run table creation on import
create_sales_table()