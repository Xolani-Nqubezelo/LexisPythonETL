# ETL/load.py
import pandas as pd
from Database.connection import get_connection

def load_dataframe(df: pd.DataFrame, table_name: str):
    """
    Load a pandas DataFrame into PostgreSQL row by row.
    Skips only the bad rows, commits valid rows.
    """
    if df.empty:
        print(f"Skipping load: {table_name} DataFrame is empty")
        return

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            print(f"--- Loading {table_name} ---")

            loaded_rows = 0
            for _, row in df.iterrows():
                try:
                    cur.execute("SAVEPOINT row_savepoint")

                    cols = list(row.index)
                    values = [row[col] for col in cols]
                    placeholders = ", ".join(["%s"] * len(cols))
                    columns_str = ", ".join(cols)
                    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    cur.execute(sql, values)

                    cur.execute("RELEASE SAVEPOINT row_savepoint")
                    loaded_rows += 1

                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_savepoint")
                    print(f"Skipping row {values}: {e}")
# I am printing out how many rows loaded
            conn.commit()
            print(f"Loaded {loaded_rows} rows into {table_name}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"ERROR loading {table_name}: {e}")

    finally:
        if conn:
            conn.close()
