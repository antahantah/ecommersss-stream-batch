{{ config(materialized='table') }}

-- SELECT
--     *
-- FROM {{ ref('stg_ecommersssusers') }}

SELECT
    user_id,
    first_name,
    email,
    -- Uncomment created_date for future project
    -- created_date AS signup_date,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_ecommersssusers') }}