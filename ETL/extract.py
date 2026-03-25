import pandas as pd

def extract_customers(path):
    return pd.read_csv(path)

def extract_orders(file_path: str) -> pd.DataFrame:
    # JSON array
    try:
        df = pd.read_json(file_path)
        print("Orders extracted:", len(df))
        return df
    except ValueError as e:
        print("Failed to read orders JSON:", e)
        return pd.DataFrame()

def extract_order_items(path):
    return pd.read_csv(path)

# orders = extract_orders("C:\\Users\\User\\Downloads\\LexisNexus\\data\\orders.jsonl")
# print(orders.head())
# print("Orders shape:", orders.shape)