{{ config( materialized = 'table', schema = 'dw_eco_essentials' ) }} 


SELECT
   o.order_id,
   p.product_key,
   c.campaign_key,
   cu.customer_key,
   d.date_key,
   ps.price,
   ol.quantity,
   ol.discount,
   ol.price_after_discount
FROM {{source('eco_essentials_landing', 'orders')}} o
INNER JOIN {{source('eco_essentials_landing', 'ORDER_LINE')}} ol ON o.order_id = ol.order_id
INNER JOIN {{source('eco_essentials_landing', 'product')}} ps ON ps.product_id = ol.product_id
INNER JOIN {{ref('e_dim_customer')}} cu ON cu.customer_id = o.customer_id
INNER JOIN {{ref('e_dim_product')}} p ON p.product_id = ol.product_id
INNER JOIN {{ref('e_dim_campaign')}} c ON c.campaign_id = ol.campaign_id
INNER JOIN {{ref('e_dim_date')}} d ON d.date = CAST(o.order_timestamp AS DATE)