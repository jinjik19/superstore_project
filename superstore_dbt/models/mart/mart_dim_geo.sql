WITH deduplicated_geo AS (
    SELECT
        DISTINCT country,
        region,
        city,
        state,
        CASE
            WHEN postal_code IS NULL THEN 'Unknown'
            ELSE toString(postal_code)
        END AS postal_code
    FROM {{ source('stg_superstore', 'stg_orders') }}
)

SELECT
    toInt64(row_number() over()) AS geo_key,
    country,
    region,
    city,
    state,
    postal_code
FROM deduplicated_geo

UNION ALL

SELECT
    toInt64(0) AS geo_key,
    toString('Unknown') AS country,
    toString('Unknown') AS region,
    toString('Unknown') AS city,
    toString('Unknown') AS state,
    toString('Unknown') AS postal_code