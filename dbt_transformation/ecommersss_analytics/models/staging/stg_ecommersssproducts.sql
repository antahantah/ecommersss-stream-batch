-- SELECT * FROM {{ source('streaming_raw', 'ecommersssProducts') }}

SELECT DISTINCT
    product_id,
    product_name,
    category,
    price,
    updated_at,
FROM {{ source('streaming_raw', 'ecommersssProducts') }}