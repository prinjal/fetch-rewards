{{ config(
    materialized='table',
    unique_key='user_id'
) }}

WITH ranked_users AS (
    SELECT 
        user_id,
        active,
        created_date,
        last_login,
        role,
        state,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_date DESC) AS row_num
    FROM 
        {{ ref('stg_users') }}
)
SELECT 
    user_id,
    active,
    created_date,
    last_login,
    role,
    state
FROM 
    ranked_users
WHERE 
    row_num = 1
