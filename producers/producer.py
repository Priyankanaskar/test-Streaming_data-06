import json
import random
import time
from datetime import datetime

# Sample product categories
CATEGORIES = ["Electronics", "Clothing", "Groceries", "Home & Kitchen", "Toys", "Sports"]

def generate_transaction():
    """Generate a fake transaction record."""
    transaction = {
        "transaction_id": f"T{random.randint(1000, 9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": f"C{random.randint(1, 500)}",
        "product_category": random.choice(CATEGORIES),
        "purchase_amount": round(random.uniform(5.0, 500.0), 2),
        "payment_method": random.choice(["Credit Card", "PayPal", "Cryptocurrency"]),
        "region": random.choice(["North America", "Europe", "Asia", "Australia"])
    }
    return transaction

def write_transactions_to_file():
    """Continuously generate transactions and write to a JSON file."""
    while True:
        transaction = generate_transaction()
        print(f"Generated Transaction: {transaction}")
        
        # Append transaction to a JSON file
        with open("data/transactions.json", "a") as f:
            f.write(json.dumps(transaction) + "\n")

        time.sleep(2)  # Generates a transaction every 2 seconds

if __name__ == "__main__":
    write_transactions_to_file()