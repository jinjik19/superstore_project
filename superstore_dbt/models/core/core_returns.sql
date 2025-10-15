SELECT 
    DISTINCT order_id,
    CASE
        WHEN returned = 'Yes' THEN TRUE
        ELSE FALSE
    END AS returned
FROM {{source('stg_superstore', 'stg_returns')}}