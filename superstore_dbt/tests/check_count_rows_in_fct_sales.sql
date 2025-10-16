SELECT COUNT(*) AS row_count
FROM {{ ref('mart_fct_sales') }}
HAVING row_count != 9994