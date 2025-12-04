{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_ecommersssorders') }}
),

users AS (
    SELECT * FROM {{ ref('dim_ecommersssusers') }}
),

flag_users AS (
    SELECT
        user_id,
        COUNT(order_id) as total_orders, -- cek berapa kali transaksi
        -- SUM(is_fraud) as fraud_orders_count, -- cek berapa yang fraud
        COUNTIF(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        SUM(total_amount) as total_purchasing,
        SUM(CASE 
                WHEN is_fraud THEN total_amount
                ELSE 0
        END) as total_fraud_loss

    FROM orders 
    GROUP BY user_id
)

SELECT
    fl.user_id,

    u.first_name,
    u.email,

    fl.total_orders,
    FORMAT("%'d", fl.total_purchasing) as total_transaction,
    -- fl.total_purchasing,
    fl.fraud_orders_count,
    FORMAT("%'d", fl.total_fraud_loss) as fake_transaction,
    -- fl.total_fraud_loss,

    CASE 
        WHEN fl.fraud_orders_count >= 3 THEN 'Blacklist'
        WHEN fl.fraud_orders_count > 0 THEN 'Watchlist'
        ELSE 'Good Standing'
    END AS blacklist_status

FROM flag_users AS fl
LEFT JOIN users AS u ON fl.user_id = u.user_id
WHERE fl.fraud_orders_count > 0
ORDER BY fl.fraud_orders_count DESC, fl.total_orders DESC