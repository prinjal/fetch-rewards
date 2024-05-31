import json
import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()
# Database connection parameters
conn_params = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": 5432
}

# SQL statements to create raw tables
create_table_statements = {
    "raw_users": """
        DROP TABLE IF EXISTS raw_users;
        CREATE TABLE IF NOT EXISTS raw_users (
            raw_data JSONB
        );
    """,
    "raw_brands": """
        DROP TABLE IF EXISTS raw_brands;
        CREATE TABLE IF NOT EXISTS raw_brands (
            raw_data JSONB
        );
    """,
    "raw_receipts": """
        DROP TABLE IF EXISTS raw_receipts;
        CREATE TABLE IF NOT EXISTS raw_receipts (
            raw_data JSONB
        );
    """
}

def create_tables():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        for table_name, create_statement in create_table_statements.items():
            cur.execute(create_statement)
            print(f"Table {table_name} created successfully.")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating tables: {e}")

def check_table_exists(table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(f"SELECT to_regclass('{table_name}');")
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Error checking if table {table_name} exists: {e}")
        return False

def load_json_to_raw_table(json_file, table_name):
    if not check_table_exists(table_name):
        print(f"Table {table_name} does not exist. Skipping data load.")
        return

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        with open(json_file, 'r') as file:
            for line in file:
                record = json.loads(line.strip())
                query = f"INSERT INTO {table_name} (raw_data) VALUES (%s)"
                cur.execute(query, [json.dumps(record)])
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Data from {json_file} loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into table {table_name}: {e}")

# Create raw tables
create_tables()

# Load JSON data into raw tables
load_json_to_raw_table('data/users.json', 'raw_users')
load_json_to_raw_table('data/brands.json', 'raw_brands')
load_json_to_raw_table('data/receipts.json', 'raw_receipts')