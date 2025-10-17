WITH deduplicated_shipping AS (
    SELECT DISTINCT ship_mode
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt32(row_number() over()) AS ship_key,
    ship_mode
FROM deduplicated_shipping