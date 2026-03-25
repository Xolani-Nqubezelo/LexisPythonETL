Database:
<img width="813" height="399" alt="image" src="https://github.com/user-attachments/assets/775a5383-99bf-425d-bceb-10bf5efa3113" />﻿

# ETL Project: Orders Pipeline

This project is a Python-based ETL pipeline for processing **customers, orders, and order items** data.  
It extracts data from CSV files, transforms it to enforce data quality, and loads it into a PostgreSQL database.  
The project also includes SQL views for analytics and data validation.

## **Project Structure**
- `ETL/` – Contains extraction, transformation, and loading logic  
- `Database/` – Connection helper and schema SQL  
- `main.py` – Entry point to run the ETL pipeline  using this command:  python main.py run 
- `config.yaml` – File paths for source data files
- `data` - contains 2 csv files and 1 json

## **Prerequisites**

- Python 3.9+
- PostgreSQL 12+
- Required Python packages:

```bash
pip install pandas psycopg2-binary pyyaml
PostgreSQL database configured with access credentials (update Database/connection.py)
Setup
Configure database connection in Database/connection.py.
Update file paths in config.yaml:

files:
  customers: "data/customers.csv"
  orders: "data/orders.csv"
  order_items: "data/order_items.csv"

  
database:
  host: "localhost"
  port: 5432
  user: "your_db_user"
  password: "your_db_password"
  database: "orders_db"


Postgres database:
  
Initialize the database schema:
python main.py init
Run ETL Pipeline
python main.py run
Extracts CSV data from configured paths
Transforms customers, orders, and order items with data quality rules
Loads data into PostgreSQL tables
Prints summary of records loaded and skipped
Analytics Views

After running the ETL, you can query the following views:

daily_metrics – Orders count, total revenue, and average order value per day
top_customers – Top 10 customers by lifetime spend
top_skus – Top 10 SKUs by revenue and units sold
duplicate_customers – Identifies duplicate customers by lowercase email
orders_missing_customers – Orders referencing missing customers
invalid_order_items – Order items with non-positive quantities or unit prices
invalid_order_status – Orders with invalid status

Example query:

SELECT * FROM daily_metrics;
Notes
The ETL pipeline handles foreign key constraints and basic data quality.
Rows failing validation (e.g., invalid customer IDs, non-positive unit prices) are skipped but logged.
Tables are truncated before loading to ensure fresh data.
Contributing
Clone the repo, create a new branch, and submit PRs for improvements.
Ensure data validation rules are preserved.
