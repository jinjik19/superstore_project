WITH deduplicated_shipping AS (
    SELECT DISTINCT ship_mode
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt32(row_number() over()) AS ship_key,
    ship_mode
FROM deduplicated_shipping

UNION ALL

SELECT
    toInt32(0) AS ship_key,
    toString('Unknown') AS ship_mode