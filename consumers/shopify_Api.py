import shopify
import os

# Set your Shopify store credentials here
SHOP_URL = 'your-shop-name.myshopify.com'
API_KEY = 'your-api-key'
PASSWORD = 'your-api-password'

# Authenticate with Shopify
shopify.ShopifyResource.set_site(f'https://{API_KEY}:{PASSWORD}@{SHOP_URL}/admin')

# Fetch orders (e.g., sales data)
def get_shopify_orders():
    orders = shopify.Order.find(status="any", created_at_min="2025-01-01T00:00:00-00:00")
    sales_data = []
    
    for order in orders:
        order_data = {
            'order_id': order.id,
            'total_price': float(order.total_price),
            'created_at': order.created_at,
            'customer_name': order.customer.get('first_name', '') + ' ' + order.customer.get('last_name', ''),
            'line_items': [{'name': item.name, 'quantity': item.quantity, 'price': float(item.price)} for item in order.line_items]
        }
        sales_data.append(order_data)
    
    return sales_data
