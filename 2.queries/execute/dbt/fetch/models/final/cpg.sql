{{ config(
    materialized='table',
    unique_key='cpg_id'
) }}

WITH ranked_cpg AS (
    SELECT 
        cpg_id,
        cpg_ref,
        ROW_NUMBER() OVER (PARTITION BY cpg_id ORDER BY cpg_id) AS row_num
    FROM 
        {{ ref('stg_cpg') }}
)
SELECT 
    cpg_id,
    cpg_ref
FROM 
    ranked_cpg
WHERE 
    row_num = 1
