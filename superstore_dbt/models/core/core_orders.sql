SELECT
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    CASE
        WHEN city = 'Burlington' AND postal_code IS NULL THEN '05401'
        ELSE 'Unknown'
    END AS postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit
FROM {{ source('stg_superstore', 'stg_orders') }}
