{{ config(materialized='table') }}

WITH raw_users AS (
    SELECT 
        raw_data->>'_id' AS user_id_raw,
        raw_data->>'active' AS active_raw,
        raw_data->>'createdDate' AS created_date_raw,
        raw_data->>'lastLogin' AS last_login_raw,
        raw_data->>'role' AS role_raw,
        raw_data->>'state' AS state
    FROM 
        {{ source('raw', 'raw_users') }}
),
transformed_users AS (
    SELECT
        (user_id_raw::jsonb->>'$oid')::varchar(255) AS user_id,
        active_raw::boolean AS active,
        TO_TIMESTAMP((created_date_raw::jsonb->>'$date')::bigint / 1000) AS created_date,
        TO_TIMESTAMP((last_login_raw::jsonb->>'$date')::bigint / 1000) AS last_login,
        role_raw::varchar(255) AS role,
        state::varchar(255) AS state
    FROM
        raw_users
)

SELECT * FROM transformed_users