import json
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import uuid

conn_params = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "db"),   
    "port": 5432
}

# SQL statements to create tables
create_table_statements = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            active BOOLEAN,
            created_date TIMESTAMP,
            last_login TIMESTAMP,
            role VARCHAR(50),
            sign_up_source VARCHAR(50),
            state VARCHAR(2)
        );
    """,
    "brands": """
        CREATE TABLE IF NOT EXISTS brands (
            brand_id UUID PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(255),
            barcode VARCHAR(255),
            brand_code VARCHAR(255) UNIQUE,
            cpg_id UUID,
            top_brand BOOLEAN,
            category_code VARCHAR(255)
        );
    """,
    "receipts": """
        CREATE TABLE IF NOT EXISTS receipts (
            receipt_id UUID PRIMARY KEY,
            bonus_points_earned INT,
            bonus_points_earned_reason VARCHAR(255),
            create_date TIMESTAMP,
            date_scanned TIMESTAMP,
            finished_date TIMESTAMP,
            modify_date TIMESTAMP,
            points_awarded_date TIMESTAMP,
            points_earned NUMERIC(10, 2),
            purchase_date TIMESTAMP,
            purchased_item_count INT,
            rewards_receipt_status VARCHAR(50),
            total_spent NUMERIC(10, 2),
            user_id UUID REFERENCES users(user_id)
        );
    """,
    "cpg": """
        CREATE TABLE IF NOT EXISTS cpg (
            cpg_id UUID PRIMARY KEY,
            name VARCHAR(255)
        );
    """,
    "receipt_items": """
        CREATE TABLE IF NOT EXISTS receipt_items (
            receipt_item_id UUID PRIMARY KEY,
            receipt_id UUID REFERENCES receipts(receipt_id),
            barcode VARCHAR(255),
            brand_code VARCHAR(255) REFERENCES brands(brand_code) ,
            description VARCHAR(255),
            discounted_item_price NUMERIC(10, 2),
            final_price NUMERIC(10, 2),
            item_price NUMERIC(10, 2),
            needs_fetch_review BOOLEAN,
            needs_fetch_review_reason VARCHAR(255),
            original_receipt_item_text TEXT,
            partner_item_id UUID,
            points_earned INT,
            points_payer_id UUID,
            price_after_coupon NUMERIC(10, 2),
            prevent_target_gap_points BOOLEAN,
            quantity_purchased INT,
            rewards_group VARCHAR(255),
            rewards_product_partner_id UUID,
            target_price NUMERIC(10, 2),
            user_flagged_barcode VARCHAR(255),
            user_flagged_description VARCHAR(255),
            user_flagged_new_item BOOLEAN,
            user_flagged_price NUMERIC(10, 2),
            user_flagged_quantity INT
        );
    """
}

# Define column mappings to handle nested fields
column_mappings_users = {
    'user_id': ['_id', '$oid'],
    'active': ['active'],
    'created_date': ['createdDate', '$date'],
    'last_login': ['lastLogin', '$date'],
    'role': ['role'],
    'sign_up_source': ['signUpSource'],
    'state': ['state']
}

column_mappings_brands = {
    'brand_id': ['_id', '$oid'],
    'name': ['name'],
    'category': ['category'],
    'barcode': ['barcode'],
    'brand_code': ['brandCode'],
    'cpg_id': ['cpg', '$id', '$oid'],
    'top_brand': ['topBrand'],
    'category_code': ['categoryCode']
}

column_mappings_receipts = {
    'receipt_id': ['_id', '$oid'],
    'bonus_points_earned': ['bonusPointsEarned'],
    'bonus_points_earned_reason': ['bonusPointsEarnedReason'],
    'create_date': ['createDate', '$date'],
    'date_scanned': ['dateScanned', '$date'],
    'finished_date': ['finishedDate', '$date'],
    'modify_date': ['modifyDate', '$date'],
    'points_awarded_date': ['pointsAwardedDate', '$date'],
    'points_earned': ['pointsEarned'],
    'purchase_date': ['purchaseDate', '$date'],
    'purchased_item_count': ['purchasedItemCount'],
    'rewards_receipt_status': ['rewardsReceiptStatus'],
    'total_spent': ['totalSpent'],
    'user_id': ['userId']
}

column_mappings_receipt_items = {
    'receipt_item_id': [],
    'receipt_id': [],
    'barcode': ['barcode'],
    'brand_code':['brandCode'],
    'description': ['description'],
    'discounted_item_price': ['discountedItemPrice'],
    'final_price': ['finalPrice'],
    'item_price': ['itemPrice'],
    'needs_fetch_review': ['needsFetchReview'],
    'needs_fetch_review_reason': ['needsFetchReviewReason'],
    'original_receipt_item_text': ['originalReceiptItemText'],
    'partner_item_id': ['partnerItemId', '$oid'],
    'points_earned': ['pointsEarned'],
    'points_payer_id': ['pointsPayerId', '$oid'],
    'price_after_coupon': ['priceAfterCoupon'],
    'prevent_target_gap_points': ['preventTargetGapPoints'],
    'quantity_purchased': ['quantityPurchased'],
    'rewards_group': ['rewardsGroup'],
    'rewards_product_partner_id': ['rewardsProductPartnerId', '$oid'],
    'target_price': ['targetPrice'],
    'user_flagged_barcode': ['userFlaggedBarcode'],
    'user_flagged_description': ['userFlaggedDescription'],
    'user_flagged_new_item': ['userFlaggedNewItem'],
    'user_flagged_price': ['userFlaggedPrice'],
    'user_flagged_quantity': ['userFlaggedQuantity']
}

def create_tables():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        for table_name, create_statement in create_table_statements.items():
            cur.execute(create_statement)
            print(f"Table {table_name} is created successfully.")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating tables: {e}")

def convert_to_timestamp(ms):
    return datetime.utcfromtimestamp(ms / 1000.0) if ms is not None else None

def get_nested_value(record, keys):
    try:
        value = record
        for key in keys:
            if key in ["pointsEarned","total_spent","discounted_item_price","final_price","item_price","price_after_coupon","target_price","user_flagged_price"]:
                return float(value.get(key,0))
            if key in ["$date"]:
                value = convert_to_timestamp(value["$date"])
                return value
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    except Exception as e:
        return None


def convert_to_valid_uuid(value):
    try:
        if len(value) == 32:
            return str(uuid.UUID(value))
        elif len(value) < 32:
            value = value.ljust(32, '0')  # Pad with zeros to make it 32 characters long
            return str(uuid.UUID(value))
        else:
            raise ValueError("The value is too long to be a UUID.")
    except ValueError as e:
        # print(f"Error converting to UUID: {e}")
        return uuid.uuid4()


def load_json_lines_to_table(json_file, table_name, columns, column_mappings):
    with open(json_file, 'r') as file:
        for line in file:
            try:
                record = json.loads(line.strip())
                processed_record = []
                for col in columns:
                    value = get_nested_value(record, column_mappings[col])
                    if col.endswith("_id") and value:
                        value = convert_to_valid_uuid(value)
                    processed_record.append(value)

                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                cur.execute(query, processed_record)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error inserting data into table {table_name}: {e}")



# Load brands next
def load_brands_and_cpg(json_file):
    with open(json_file, 'r') as file:
        cpg_records = set()
        for line in file:
            try:
                record = json.loads(line.strip())
                cpg_record = get_nested_value(record, ['cpg'])
                if cpg_record:
                    cpg_id = convert_to_valid_uuid(cpg_record.get('$id', {}).get('$oid', None))
                    cpg_name = cpg_record.get('name')
                    cpg_records.add((cpg_id, cpg_name))
                processed_record = []
                for col in column_mappings_brands:
                    value = get_nested_value(record, column_mappings_brands[col])
                    if col.endswith("_id") and value:
                        value = convert_to_valid_uuid(value)
                    processed_record.append(value)

                query = f"INSERT INTO brands ({', '.join(column_mappings_brands.keys())}) VALUES ({', '.join(['%s'] * len(column_mappings_brands))})"
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                cur.execute(query, processed_record)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error inserting data into table brands: {e}")

        # Insert CPG records
        if cpg_records:
            cpg_query = "INSERT INTO cpg (cpg_id, name) VALUES %s ON CONFLICT DO NOTHING"
            try:
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                execute_values(cur, cpg_query, list(cpg_records))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error inserting data into table cpg: {e}")




def load_receipts_and_items(json_file):
    with open(json_file, 'r') as file:
        for line in file:
            try:
                record = json.loads(line.strip())
                receipt_items_records = []
                receipt_items = record.get('rewardsReceiptItemList', [])
                receipt_id = convert_to_valid_uuid(record.get('_id', {}).get('$oid', None))
                for item in receipt_items:
                    receipt_item_record = [str(uuid.uuid4()), receipt_id]  # Generate a new UUID for receipt_item_id and set receipt_id
                    for col in column_mappings_receipt_items:
                        if col not in ['receipt_item_id', 'receipt_id']:
                            value = get_nested_value(item, column_mappings_receipt_items[col])
                            if col.endswith("_id") and value:
                                value = convert_to_valid_uuid(value)
                            receipt_item_record.append(value)
                    receipt_items_records.append(receipt_item_record)

                processed_record = []
                for col in column_mappings_receipts:
                    value = get_nested_value(record, column_mappings_receipts[col])
                    if col.endswith("_id") and value:
                        value = convert_to_valid_uuid(value)
                    processed_record.append(value)

                print(len(processed_record))

                receipt_query = f"INSERT INTO receipts ({', '.join(column_mappings_receipts.keys())}) VALUES ({', '.join(['%s'] * len(column_mappings_receipts))})"
                conn = psycopg2.connect(**conn_params)
                cur = conn.cursor()
                cur.execute(receipt_query, processed_record)
                conn.commit()
                
                # Insert receipt items
                if receipt_items_records:
                    receipt_items_query = f"INSERT INTO receipt_items ({', '.join(['receipt_item_id', 'receipt_id'] + [col for col in column_mappings_receipt_items if col not in ['receipt_item_id', 'receipt_id']])}) VALUES %s"
                    execute_values(cur, receipt_items_query, receipt_items_records)
                    conn.commit()
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error inserting data into table receipts or receipt_items: {e}")


create_tables()

# Load users first
load_json_lines_to_table('/data/users.json', 'users', [
    'user_id', 'active', 'created_date', 'last_login', 'role', 'sign_up_source', 'state'
], column_mappings_users)

load_brands_and_cpg('/data/brands.json')

load_receipts_and_items('/data/receipts.json')