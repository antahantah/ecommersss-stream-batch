{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_ecommersssorders') }}
),

products AS (
    SELECT * FROM {{ ref('dim_ecommersssproducts') }}
),

users AS (
    SELECT * FROM {{ ref('dim_ecommersssusers') }}
),

habit_orders AS (
    SELECT
        user_id,
        product_id,

        COUNT(order_id) as total_orders, -- cek berapa kali transaksi
        -- SUM(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        COUNTIF(NOT is_fraud) as genuine_orders_count, -- cek berapa yang genuine
        COUNTIF(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        MAX(order_date) as new_transaction

    FROM orders 
    GROUP BY user_id, product_id
)

SELECT
    hbt.user_id,
    u.first_name,
    u.email,
    hbt.product_id,
    p.product_name,

    hbt.total_orders,    
    hbt.genuine_orders_count,
    hbt.fraud_orders_count,
    -- hbt.latest_transaction
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", hbt.new_transaction, "Asia/Jakarta") AS latest_transaction

FROM habit_orders AS hbt
LEFT JOIN products AS p ON hbt.product_id = p.product_id
LEFT JOIN users AS u ON hbt.user_id = u.user_id
-- sorting order terbanyak, lalu users non Null, terakhir user id jika nama depan sama
ORDER BY hbt.total_orders DESC, u.first_name NULLS LAST, hbt.user_id 
