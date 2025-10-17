WITH transform_orders AS (
    SELECT 
        DISTINCT order_id,
        CASE 
            WHEN r.returned IS NULL then toBool(0) 
            ELSE toBool(1)
        END AS returned
    FROM {{ source('stg_superstore', 'stg_orders') }} o
    LEFT JOIN {{ source('stg_superstore', 'stg_returns') }} r on r.order_id = o.order_id
)

SELECT
    toInt64(row_number() over()) AS order_key,
    order_id,
    returned
FROM transform_orders