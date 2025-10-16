WITH transform_orders AS (
    SELECT 
        DISTINCT order_id,
        CASE 
            WHEN r.returned IS NULL then toBool(0) 
            ELSE toBool(1)
        END AS returned,
        toInt32(formatDateTime(o.order_date, '%Y%m%d')) AS order_date_key,
        toInt32(formatDateTime(o.ship_date, '%Y%m%d')) AS ship_date_key
    FROM {{ source('stg_superstore', 'stg_orders') }} o
    LEFT JOIN {{ source('stg_superstore', 'stg_returns') }} r on r.order_id = o.order_id
)

SELECT
    toInt64(row_number() over()) AS order_key,
    order_id,
    returned,
    order_date_key,
    ship_date_key
FROM transform_orders

UNION ALL

SELECT
    toInt64(0) AS order_key,
    toString('Unknown') AS order_id,
    toBool(0) AS returned,
    toInt32(-1) AS order_date_key,
    toInt32(-1) AS ship_date_key