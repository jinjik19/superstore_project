SELECT
    row_number() over() as sales_key,
    o.sales,
    o.quantity,
	o.profit,
	o.discount,
    mdo.order_key,
    mdp.product_key,
    mdg.geo_key,
    mdc.customer_key,
    mds.ship_key,
    mdm.manager_key,
    mdcr1.date_key AS order_date_key,
    mdcr2.date_key AS ship_date_key
FROM stg_orders o
INNER JOIN {{ ref('mart_dim_orders') }} mdo ON mdo.order_id = o.order_id
INNER JOIN {{ ref('mart_dim_product') }} mdp 
    ON mdp.product_name = o.product_name AND mdp.segment = o.segment 
        AND mdp.sub_category = o.sub_category AND mdp.category = o.category
        AND mdp.product_id = o.product_id
INNER JOIN {{ ref('mart_dim_geo') }} mdg 
    ON mdg.country = o.country AND mdg.city = o.city
    AND mdg.state = o.state 
    AND (mdg.postal_code = toString(o.postal_code) OR (o.postal_code IS NULL AND mdg.postal_code = 'Unknown'))
INNER JOIN {{ ref('mart_dim_customer') }} mdc 
    ON mdc.customer_id = o.customer_id AND mdc.customer_name = o.customer_name
INNER JOIN {{ ref('mart_dim_shipping') }} mds ON mds.ship_mode = o.ship_mode
INNER JOIN {{ ref('mart_dim_managers') }} mdm ON mdm.region = o.region
INNER JOIN {{ ref('mart_dim_calendar') }} mdcr1 
    ON mdcr1.date_key = toInt32(formatDateTime(o.order_date, '%Y%m%d'))
INNER JOIN {{ ref('mart_dim_calendar') }} mdcr2
    ON mdcr2.date_key = toInt32(formatDateTime(o.ship_date, '%Y%m%d'))