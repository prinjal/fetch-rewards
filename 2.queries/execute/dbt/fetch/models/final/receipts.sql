{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT 
        receipt_id,
        bonus_points_earned,
        bonus_points_earned_reason,
        create_date,
        date_scanned,
        finished_date,
        modify_date,
        points_awarded_date,
        points_earned,
        purchase_date,
        purchased_item_count,
        rewards_receipt_status,
        total_spent,
        user_id
    FROM {{ ref('stg_receipts') }}
)

, validated_users AS (
    SELECT
        base.*
    FROM
        base
    JOIN
        {{ ref('users') }} u ON base.user_id = u.user_id
)

SELECT
    receipt_id,
    bonus_points_earned,
    bonus_points_earned_reason,
    create_date,
    date_scanned,
    finished_date,
    modify_date,
    points_awarded_date,
    points_earned,
    purchase_date,
    purchased_item_count,
    rewards_receipt_status,
    total_spent,
    user_id
FROM validated_users