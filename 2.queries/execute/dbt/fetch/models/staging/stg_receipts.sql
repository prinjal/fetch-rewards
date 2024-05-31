{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT 
        raw_data->>'_id' AS receipt_id_raw,
        raw_data->>'bonusPointsEarned' AS bonus_points_earned,
        raw_data->>'bonusPointsEarnedReason' AS bonus_points_earned_reason,
        TO_TIMESTAMP((raw_data->'createDate'->>'$date')::bigint / 1000) AS create_date,
        TO_TIMESTAMP((raw_data->'dateScanned'->>'$date')::bigint / 1000) AS date_scanned,
        TO_TIMESTAMP((raw_data->'finishedDate'->>'$date')::bigint / 1000) AS finished_date,
        TO_TIMESTAMP((raw_data->'modifyDate'->>'$date')::bigint / 1000) AS modify_date,
        TO_TIMESTAMP((raw_data->'pointsAwardedDate'->>'$date')::bigint / 1000) AS points_awarded_date,
        raw_data->>'pointsEarned' AS points_earned,
        TO_TIMESTAMP((raw_data->'purchaseDate'->>'$date')::bigint / 1000) AS purchase_date,
        raw_data->>'purchasedItemCount' AS purchased_item_count,
        raw_data->>'rewardsReceiptStatus' AS rewards_receipt_status,
        raw_data->>'totalSpent' AS total_spent,
        raw_data->>'userId' AS user_id
    FROM 
        {{ source('raw', 'raw_receipts') }}
)

SELECT
    (receipt_id_raw::jsonb->>'$oid')::varchar(255) AS receipt_id,
    bonus_points_earned::integer AS bonus_points_earned,
    bonus_points_earned_reason::varchar(255) AS bonus_points_earned_reason,
    create_date,
    date_scanned,
    finished_date,
    modify_date,
    points_awarded_date,
    points_earned::numeric(10,2) AS points_earned,
    purchase_date,
    purchased_item_count::integer AS purchased_item_count,
    rewards_receipt_status::varchar(50) AS rewards_receipt_status,
    total_spent::numeric(10,2) AS total_spent,
    user_id::varchar(255) AS user_id
FROM raw_data
