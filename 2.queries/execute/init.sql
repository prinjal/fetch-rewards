CREATE DATABASE fetch_rewards_exercise;

\c fetch_rewards_exercise;

-- Create CPG Table first
CREATE TABLE IF NOT EXISTS cpg (
    cpg_id UUID PRIMARY KEY,
    name VARCHAR(255)
);

-- Create Users Table
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    active BOOLEAN,
    created_date TIMESTAMP,
    last_login TIMESTAMP,
    role VARCHAR(50),
    sign_up_source VARCHAR(50),
    state VARCHAR(2)
);

-- Create Brands Table
CREATE TABLE IF NOT EXISTS brands (
    brand_id UUID PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255),
    barcode VARCHAR(255),
    brand_code VARCHAR(255) UNIQUE,
    cpg_id UUID,
    top_brand BOOLEAN,
    category_code VARCHAR(255),
    FOREIGN KEY (cpg_id) REFERENCES cpg (cpg_id)
);

-- Create Receipts Table
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id UUID PRIMARY KEY,
    bonus_points_earned INTEGER,
    bonus_points_earned_reason VARCHAR(255),
    create_date TIMESTAMP,
    date_scanned TIMESTAMP,
    finished_date TIMESTAMP,
    modify_date TIMESTAMP,
    points_awarded_date TIMESTAMP,
    points_earned INTEGER,
    purchase_date TIMESTAMP,
    purchased_item_count INTEGER,
    rewards_receipt_status VARCHAR(50),
    total_spent NUMERIC(10, 2),
    user_id UUID,
    FOREIGN KEY (user_id) REFERENCES users (user_id)
);

-- Create ReceiptItems Table
CREATE TABLE IF NOT EXISTS receipt_items (
    receipt_item_id UUID PRIMARY KEY,
    receipt_id UUID,
    barcode VARCHAR(255),
    brand_id UUID,
    description VARCHAR(255),
    discounted_item_price NUMERIC(10, 2),
    final_price NUMERIC(10, 2),
    item_price NUMERIC(10, 2),
    needs_fetch_review BOOLEAN,
    needs_fetch_review_reason VARCHAR(255),
    original_receipt_item_text TEXT,
    partner_item_id UUID,
    points_earned INTEGER,
    points_payer_id UUID,
    price_after_coupon NUMERIC(10, 2),
    prevent_target_gap_points BOOLEAN,
    quantity_purchased INTEGER,
    rewards_group VARCHAR(255),
    rewards_product_partner_id UUID,
    target_price NUMERIC(10, 2),
    user_flagged_barcode VARCHAR(255),
    user_flagged_description VARCHAR(255),
    user_flagged_new_item BOOLEAN,
    user_flagged_price NUMERIC(10, 2),
    user_flagged_quantity INTEGER,
    FOREIGN KEY (receipt_id) REFERENCES receipts (receipt_id),
    FOREIGN KEY (brand_id) REFERENCES brands (brand_id),
    FOREIGN KEY (points_payer_id) REFERENCES users (user_id)
);
