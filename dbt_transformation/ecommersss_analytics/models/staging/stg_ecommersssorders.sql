-- SELECT * FROM {{ source('streaming_raw', 'ecommersssOrders') }}

SELECT DISTINCT
    order_id,
    user_id,
    product_id,
    quantity,
    SAFE_CAST(REGEXP_REPLACE(amount, r'[^0-9]', '') AS INT64) as total_amount,
    payment_method,
    country,
    created_date,
    status,
FROM {{ source('streaming_raw', 'ecommersssOrders') }}