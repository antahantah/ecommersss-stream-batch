{{ config(materialized='table') }}

-- SELECT
--     order_id,
--     user_id,      -- Join Key to Users
--     product_id,   -- Join Key to Products
--     created_date,
--     amount,
--     status        -- Needed for  Fraud Logic later
-- FROM {{ ref('stg_ecommersssorders') }}

SELECT
    order_id,

    -- Foreign Key
    user_id,
    product_id,

    quantity,
    total_amount,    
    payment_method,    
    country,
    
    -- Standardize Status
    LOWER(status) AS order_status,
    
    -- Create a Boolean Fraud Flag
    CASE 
        WHEN LOWER(status) = 'fraud' THEN TRUE 
        ELSE FALSE 
    END AS is_fraud,
    
    created_date AS order_date,
    CURRENT_TIMESTAMP AS updated_at

FROM {{ ref('stg_ecommersssorders') }}