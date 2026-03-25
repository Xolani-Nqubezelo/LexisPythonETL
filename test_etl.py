import pandas as pd
from ETL.transform import transform_customers, transform_orders, transform_order_items

# =========================
# File paths
# =========================
CUSTOMERS_FILE = "data/customers.csv"
ORDERS_FILE = "data/orders.jsonl"
ORDER_ITEMS_FILE = "data/order_items.csv"

# =========================
# Extract Step
# =========================
print("=== EXTRACT STEP ===")

# Customers
try:
    customers = pd.read_csv(CUSTOMERS_FILE)
    print(f"Loaded customers: {len(customers)} rows")
except Exception as e:
    print(f"ERROR loading customers: {e}")
    customers = pd.DataFrame()

# Orders
try:
    orders = pd.read_json(ORDERS_FILE)
    print(f"Loaded orders: {len(orders)} rows")
except Exception as e:
    print(f"ERROR loading orders: {e}")
    orders = pd.DataFrame()

# Order Items
try:
    items = pd.read_csv(ORDER_ITEMS_FILE)
    print(f"Loaded order_items: {len(items)} rows")
except Exception as e:
    print(f"ERROR loading order_items: {e}")
    items = pd.DataFrame()

# =========================
# Transform Step
# =========================
print("\n=== TRANSFORM STEP ===")

if not customers.empty:
    customers = transform_customers(customers)

if not orders.empty and not customers.empty:
    orders = transform_orders(orders, customers['customer_id'])

if not items.empty:
    items = transform_order_items(items)

# =========================
# Print final results
# =========================
print("\n=== FINAL DATAFRAMES ===")

print("\nCustomers:")
print(customers)

print("\nOrders:")
print(orders)

print("\nOrder Items:")
print(items)