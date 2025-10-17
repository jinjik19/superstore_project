WITH deduplicated_product AS (
    SELECT
        DISTINCT product_id,
        product_name,
        category,
        sub_category,
        segment
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt64(row_number() over()) AS product_key,
    product_id,
    product_name,
    category,
    sub_category,
    segment
FROM deduplicated_product