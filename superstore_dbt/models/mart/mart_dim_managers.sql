WITH deduplicated_stg_people AS (
    SELECT 
        DISTINCT person,
        region
    FROM {{ source('stg_superstore', 'stg_people') }} o
)

SELECT
    toInt64(row_number() over()) AS manager_key,
    person,
    region
FROM deduplicated_stg_people