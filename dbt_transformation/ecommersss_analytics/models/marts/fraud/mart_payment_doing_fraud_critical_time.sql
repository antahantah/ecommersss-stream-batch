{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_ecommersssorders') }}
),

critical_time AS (
    SELECT *
    FROM orders
    WHERE EXTRACT(HOUR FROM order_date) BETWEEN 0 AND 4 
),

payment_critical AS (
    SELECT
        payment_method,
        COUNT(order_id) as total_orders_midnight, -- cek berapa kali transaksi pada jam kritis

        -- SUM(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        COUNTIF(NOT is_fraud) as genuine_orders, -- cek berapa yang genuine
        COUNTIF(is_fraud) as fraud_orders, -- cek berapa yang fraud

        SUM(total_amount) as total_purchasing,
        SUM(CASE 
                WHEN is_fraud THEN total_amount
                ELSE 0
        END) as total_fraud_loss

    FROM critical_time 
    GROUP BY payment_method
)

SELECT 
    payment_method,
    total_orders_midnight,
    genuine_orders,
    fraud_orders,
    SAFE_DIVIDE (fraud_orders, total_orders_midnight) * 100 AS fraud_percentage,
    FORMAT("%'d", total_purchasing) as total_midnights_transaction,
    FORMAT("%'d", total_fraud_loss) as fake_transaction

FROM payment_critical
ORDER BY fraud_orders DESC