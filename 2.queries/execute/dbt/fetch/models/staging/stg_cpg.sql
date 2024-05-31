{{ config(materialized='table') }}

WITH raw_cpg AS (
    SELECT DISTINCT
        raw_data->'cpg'->>'$id' AS cpg_id,
        raw_data->'cpg'->>'$ref' AS cpg_ref
    FROM 
        {{ source('raw', 'raw_brands') }}
    WHERE 
        raw_data->'cpg' IS NOT NULL
)

SELECT 
    (cpg_id::jsonb->>'$oid')::varchar(255) AS cpg_id,
    cpg_ref::varchar(255) AS cpg_ref
FROM 
    raw_cpg