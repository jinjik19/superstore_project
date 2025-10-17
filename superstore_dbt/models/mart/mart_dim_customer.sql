WITH deduplicated_customers AS (
    SELECT DISTINCT customer_id, customer_name
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt64(row_number() over()) AS customer_key,
    customer_id,
    customer_name
FROM deduplicated_customers