# main.py
import sys
import yaml
import pandas as pd

from Database.connection import get_connection
from ETL.extract import extract_customers, extract_orders, extract_order_items
from ETL.transform import transform_customers, transform_orders, transform_order_items
from ETL.load import load_dataframe


# =========================
# Load Config
# =========================
def load_config():
    try:
        with open("config.yaml") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Failed to load config.yaml -> {e}")
        sys.exit(1)


# =========================
# INIT (Schema Setup)
# =========================
def init():
    print("=== INIT STEP ===")

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        with open("Database/schema.sql") as f:
            schema_sql = f.read()

        cur.execute(schema_sql)
        conn.commit()

        print("✅ Schema created successfully")

    except Exception as e:
        print(f"ERROR initializing schema: {e}")
        if conn:
            conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# =========================
# RUN ETL
# =========================
def run(files_config):
    print("=== EXTRACT STEP ===")
    # Extract
    customers = extract_customers(files_config["customers"])
    print(f"Loaded customers: {len(customers)} rows")

    orders = extract_orders(files_config["orders"])
    print(f"Loaded orders: {len(orders)} rows")

    items = extract_order_items(files_config["order_items"])
    print(f"Loaded order_items: {len(items)} rows")

    # Transform
    print("\n=== TRANSFORM STEP ===")
    customers = transform_customers(customers)
    orders = transform_orders(orders, customers['customer_id'])
    items = transform_order_items(items, orders['order_id'].tolist())

    # Debug output
    print("\n=== FINAL DATAFRAMES ===")
    print("\nCustomers:\n", customers)
    print("\nOrders:\n", orders)
    print("\nOrder Items:\n", items)

    # Load
    print("\n=== LOAD STEP ===")
    conn = get_connection()
    with conn.cursor() as cur:
        # truncate tables respecting FK order
        cur.execute("TRUNCATE TABLE order_items CASCADE;")
        cur.execute("TRUNCATE TABLE orders CASCADE;")
        cur.execute("TRUNCATE TABLE customers CASCADE;")
        conn.commit()
        print("Tables truncated")
    conn.close()

    # Row-level load
    load_dataframe(customers, "customers")
    load_dataframe(orders, "orders")
    load_dataframe(items, "order_items")
    print("\n✅ ETL pipeline completed successfully")


# =========================
# ENTRY POINT
# =========================
def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py init | run")
        sys.exit(1)

    command = sys.argv[1].lower()
    config = load_config()

    if command == "init":
        init()
    elif command == "run":
        run(config["files"])
    else:
        print("Invalid command. Use: init | run")


if __name__ == "__main__":
    main()