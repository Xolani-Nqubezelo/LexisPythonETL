import psycopg

#import psycopg

def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="Lucia@1861",   # 🔥 replace this
        dbname="orders_db"
    )