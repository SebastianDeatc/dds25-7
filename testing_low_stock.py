import requests
import json

#Stock is 5 we try to buy 6!

# Base URLs
base_url = "http://localhost:8000"

# Create stock item
response = requests.post(f"{base_url}/stock/item/create/5")
item_id = response.json()["item_id"]
print(f"Created item with ID: {item_id}")

# Create payment user
response = requests.post(f"{base_url}/payment/create_user")
user_id = response.json()["user_id"]
print(f"Created user with ID: {user_id}")

# Add stock to item
response = requests.post(f"{base_url}/stock/add/{item_id}/1")
print(f"Added stock to item {item_id}: {response.status_code}")

# Add funds to user
response = requests.post(f"{base_url}/payment/add_funds/{user_id}/100")
print(f"Added funds to user {user_id}: {response.status_code}")

# Create order
response = requests.post(f"{base_url}/orders/create/{user_id}")
order_id = response.json()["order_id"]
print(f"Created order with ID: {order_id}")

# Add item to order
response = requests.post(f"{base_url}/orders/addItem/{order_id}/{item_id}/6")
print(f"Added item to order {order_id}: {response.status_code}")

# Find order
response = requests.get(f"{base_url}/orders/find/{order_id}")
order_details = response.json()
print(f"Order details: {json.dumps(order_details, indent=4)}")

# Checkout order
response = requests.post(f"{base_url}/orders/checkout/{order_id}")
print(f"Checkout order {order_id}: {response.status_code}")