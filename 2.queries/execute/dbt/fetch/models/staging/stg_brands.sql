{{ config(materialized='table') }}

WITH raw_brands AS (
    SELECT 
        raw_data->>'_id' AS brand_id,
        raw_data->>'barcode' AS barcode,
        raw_data->>'category' AS category,
        raw_data->>'categoryCode' AS category_code,
        raw_data->'cpg'->>'$id' AS cpg_id,
        raw_data->>'name' AS name,
        (raw_data->>'topBrand')::boolean AS top_brand
    FROM 
        {{ source('raw', 'raw_brands') }}
)

SELECT 
    (brand_id::jsonb->>'$oid')::varchar(255) AS brand_id,
    barcode::varchar(255) AS barcode,
    category::varchar(255) AS category,
    category_code::varchar(255) AS category_code,
    (cpg_id::jsonb->>'$oid')::varchar(255) AS cpg_id,
    name::varchar(255) AS name,
    top_brand
FROM 
    raw_brands
