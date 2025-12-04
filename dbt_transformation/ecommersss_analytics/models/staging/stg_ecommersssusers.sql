-- SELECT * FROM {{ source('streaming_raw', 'ecommersssUsers') }}

SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    email,
    default_payment,
    created_date,
FROM {{ source('streaming_raw', 'ecommersssUsers') }}