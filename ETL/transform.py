# ETL/transform.py
import pandas as pd

# =========================
# Transform Customers
# =========================
def transform_customers(df):
    df = df.copy()

    # Ensure country_code is valid
    df['country_code'] = df['country_code'].fillna('ZA').str[:2]

    # Drop or fix invalid emails
    df = df[df['email'].str.contains("@", na=False)]

    # Remove duplicates
    df = df.drop_duplicates(subset=['email'], keep='first')

    return df

# =========================
# Transform Orders
# =========================
# def transform_orders(df: pd.DataFrame, valid_customer_ids: pd.Series) -> pd.DataFrame:
#     df = df.copy()
#     print(f"Starting orders transformation: {len(df)} rows")

#     # Parse timestamps
#     df['order_ts'] = pd.to_datetime(df['order_ts'], errors='coerce', utc=True)

#     # Fix invalid statuses
#     valid_status = ['placed', 'shipped', 'cancelled', 'refunded']
#     invalid_mask = ~df['status'].isin(valid_status)
#     if invalid_mask.sum() > 0:
#         print(f"WARNING: Mapping {invalid_mask.sum()} invalid statuses to 'placed'")
#         df.loc[invalid_mask, 'status'] = 'placed'

#     # Only keep orders with valid customers
#     df.loc[~df['customer_id'].isin(valid_customer_ids), 'customer_id'] = None
#     missing_fk = df['customer_id'].isna().sum()
#     if missing_fk > 0:
#         print(f"WARNING: {missing_fk} orders reference unknown customers")

#     # Drop orders with invalid customer_id (prevents FK errors)
#     df = df.dropna(subset=['customer_id'])
#     df['customer_id'] = df['customer_id'].astype(int)

#     print(f"Orders transformation completed: {len(df)} rows remain")
#     return df
def transform_orders(df, valid_customer_ids):
    df = df.copy()

    # Keep only orders with valid customer_id
    df = df[df['customer_id'].isin(valid_customer_ids)]

    # Fix invalid statuses
    df['status'] = df['status'].where(df['status'].isin(['placed','shipped','cancelled','refunded']), 'placed')

    return df

# =========================
# Transform Order Items
# =========================
# def transform_order_items(df: pd.DataFrame, valid_order_ids: list) -> pd.DataFrame:
#     df = df.copy()
#     print(f"Starting order_items transformation: {len(df)} rows")

#     # Only keep rows with valid order_ids
#     df.loc[~df['order_id'].isin(valid_order_ids), 'order_id'] = None
#     missing_fk = df['order_id'].isna().sum()
#     if missing_fk > 0:
#         print(f"WARNING: {missing_fk} order_items reference unknown orders")

#     # Drop invalid rows
#     df = df.dropna(subset=['order_id'])

#     # Ensure positive quantity (check constraint)
#     df.loc[df['quantity'] < 1, 'quantity'] = 1

#     # Correct types
#     df['order_id'] = df['order_id'].astype(int)
#     df['line_no'] = df['line_no'].astype(int)
#     df['quantity'] = df['quantity'].astype(int)
#     df['unit_price'] = df['unit_price'].astype(float)

#     print(f"Order_items transformation completed: {len(df)} rows remain")
#     return df

def transform_order_items(df, valid_order_ids):
    df = df.copy()

    # Keep only items with valid order_id
    df = df[df['order_id'].isin(valid_order_ids)]

    # Ensure positive quantity and unit_price
    df = df[df['quantity'] > 0]
    df = df[df['unit_price'] > 0]

    return df