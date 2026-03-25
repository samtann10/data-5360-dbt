{{ config( materialized = 'table', schema = 'dw_eco_essentials' ) }} 

SELECT d.date_key, cu.customer_key, p.product_key, c.campaign_key, ps.price, ol.quantity, ol.discount, o.order_id 
FROM {{ source('eco_landing', 'order') }} o 
INNER JOIN {{ source('eco_landing', 'order_line') }} ol ON o.order_id = ol.order_id 
INNER JOIN {{ source('eco_landing', 'product') }} ps ON ol.product_id = ps.product_id 
INNER JOIN {{ ref('e_dim_customer') }} cu ON o.customer_id = cu.customer_id INNER JOIN {{ ref('e_dim_campaign') }} c ON ol.campaign_id = c.campaign_id 
INNER JOIN {{ ref('e_dim_product') }} p ON p.product_id = ol.product_id INNER JOIN {{ ref('e_dim_date') }} d ON d.date_day = o.order_timestamp