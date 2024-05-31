{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT
        raw_data->>'_id' AS receipt_id_raw,
        jsonb_array_elements(raw_data->'rewardsReceiptItemList') AS item,
        row_number() OVER () AS row_number
    FROM 
        {{ source('raw', 'raw_receipts') }}
)

SELECT
    md5(concat(receipt_id_raw::varchar(255), item->>'barcode', row_number)::text)::uuid AS receipt_item_id,
    (receipt_id_raw::jsonb->>'$oid')::varchar(255) AS receipt_id,
    item->>'barcode' AS barcode,
    item->>'description' AS description,
    (item->>'finalPrice')::numeric(10,2) AS final_price,
    (item->>'itemPrice')::numeric(10,2) AS item_price,
    (item->>'needsFetchReview')::boolean AS needs_fetch_review,
    item->>'needsFetchReviewReason' AS needs_fetch_review_reason,
    item->>'originalReceiptItemText' AS original_receipt_item_text,
    (item->>'quantityPurchased')::numeric(10,2) AS quantity_purchased,
    COALESCE(NULLIF(item->>'discountedItemPrice', 'null'), '0')::numeric(10,2) AS discounted_item_price,
    COALESCE(NULLIF(item->>'priceAfterCoupon', 'null'), '0')::numeric(10,2) AS price_after_coupon,
    (item->>'preventTargetGapPoints')::boolean AS prevent_target_gap_points,
    item->>'partnerItemId' AS partner_item_id,
    (item->>'pointsEarned')::numeric(10,2) AS points_earned,
    item->>'pointsPayerId' AS points_payer_id,
    item->>'rewardsGroup' AS rewards_group,
    item->>'rewardsProductPartnerId' AS rewards_product_partner_id,
    item->>'userFlaggedBarcode' AS user_flagged_barcode,
    item->>'userFlaggedDescription' AS user_flagged_description,
    (item->>'userFlaggedNewItem')::boolean AS user_flagged_new_item,
    (item->>'userFlaggedPrice')::numeric(10,2) AS user_flagged_price,
    (item->>'userFlaggedQuantity')::numeric(10,2) AS user_flagged_quantity
FROM raw_data
