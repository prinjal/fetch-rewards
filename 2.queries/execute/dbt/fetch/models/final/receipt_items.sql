{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        receipt_item_id,
        receipt_id,
        barcode,
        description,
        final_price,
        item_price,
        needs_fetch_review,
        needs_fetch_review_reason,
        original_receipt_item_text,
        quantity_purchased,
        discounted_item_price,
        price_after_coupon,
        prevent_target_gap_points,
        partner_item_id,
        points_earned,
        points_payer_id,
        rewards_group,
        rewards_product_partner_id,
        user_flagged_barcode,
        user_flagged_description,
        user_flagged_new_item,
        user_flagged_price,
        user_flagged_quantity,
        ROW_NUMBER() OVER (PARTITION BY receipt_item_id ORDER BY receipt_id) AS row_num
    FROM {{ ref('stg_receipt_items') }}
)

, validated_receipts AS (
    SELECT
        base.*
    FROM
        base
    JOIN
        {{ ref('receipts') }} r ON base.receipt_id = r.receipt_id
    WHERE base.row_num = 1
)

SELECT
    receipt_item_id,
    receipt_id,
    barcode,
    description,
    final_price,
    item_price,
    needs_fetch_review,
    needs_fetch_review_reason,
    original_receipt_item_text,
    quantity_purchased,
    discounted_item_price,
    price_after_coupon,
    prevent_target_gap_points,
    partner_item_id,
    points_earned,
    points_payer_id,
    rewards_group,
    rewards_product_partner_id,
    user_flagged_barcode,
    user_flagged_description,
    user_flagged_new_item,
    user_flagged_price,
    user_flagged_quantity
FROM validated_receipts