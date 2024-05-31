{{ config(
    materialized='table',
    unique_key='brand_id'
) }}

WITH ranked_brands AS (
    SELECT 
        brand_id,
        barcode,
        category,
        category_code,
        cpg_id,
        name,
        top_brand,
        ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY brand_id) AS row_num
    FROM 
        {{ ref('stg_brands') }}
)
SELECT 
    brand_id,
    barcode,
    category,
    category_code,
    cpg_id,
    name,
    top_brand
FROM 
    ranked_brands
WHERE 
    row_num = 1
