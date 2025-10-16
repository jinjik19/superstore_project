WITH deduplicated_customers AS (
    SELECT DISTINCT customer_id, customer_name
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt64(row_number() over()) AS customer_key,
    customer_id,
    customer_name
FROM deduplicated_customers

UNION ALL

SELECT 
    toInt64(0) AS customer_key,
    toString('Unknown') AS customer_id,
    toString('Unknown Customer') AS customer_name