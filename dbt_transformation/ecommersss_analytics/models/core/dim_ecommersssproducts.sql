{{ config(materialized='table') }}

-- SELECT
--     *
-- FROM {{ ref('stg_ecommersssproducts') }}

SELECT
    product_id,
    product_name,
    -- Uncomment Category for future project
    -- category,
    price,
    -- Very useful to see if a product is "New Arrival"
    -- Uncomment created_at for future project
    -- created_at AS product_launch_date
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_ecommersssproducts') }}