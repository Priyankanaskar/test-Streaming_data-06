from collections import defaultdict

def aggregate_sales(transactions):
    """Compute total sales per category from a list of transactions."""
    sales_summary = defaultdict(float)
    for txn in transactions:
        sales_summary[txn["product_category"]] += txn["purchase_amount"]
    return sales_summary