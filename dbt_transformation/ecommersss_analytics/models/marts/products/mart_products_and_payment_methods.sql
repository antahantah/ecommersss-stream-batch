{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('fct_ecommersssorders') }}
),

products AS (
    SELECT * FROM {{ ref('dim_ecommersssproducts') }}
),

prod_and_paym AS (
    SELECT
        product_id,
        payment_method,

        COUNT(order_id) as total_orders, -- cek berapa kali transaksi
        -- SUM(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        COUNTIF(NOT is_fraud) as genuine_orders_count, -- cek berapa yang genuine
        COUNTIF(is_fraud) as fraud_orders_count, -- cek berapa yang fraud

        SUM(total_amount) as total_purchasing,
        SUM(CASE 
                WHEN is_fraud THEN total_amount
                ELSE 0
        END) as total_fraud_loss

    FROM orders 
    GROUP BY product_id, payment_method
)

SELECT
    pnp.product_id,

    p.product_name,
    pnp.payment_method,

    pnp.total_orders,
    FORMAT("%'d", pnp.total_purchasing) as total_transaction,
    -- fl.total_purchasing,
    
    pnp.genuine_orders_count,

    pnp.fraud_orders_count,
    FORMAT("%'d", pnp.total_fraud_loss) as fake_transaction,
    -- fl.total_fraud_loss,

FROM prod_and_paym AS pnp
LEFT JOIN products AS p ON pnp.product_id = p.product_id
-- ORDER BY pnp.fraud_orders_count DESC, pnp.total_orders DESC