{{ config(materialized='table', schema='dw_eco_essentials') }}

SELECT 
    d.date_key, 
    cu.customer_key,
    p.product_key, 
    c.campaign_key, 
    ps.price, 
    ol.quantity, 
    ol.discount, 
    o.order_id 
FROM {{ source('eco_essentials_landing', 'orders') }} o
JOIN {{ source('eco_essentials_landing', 'order_line') }} ol 
    ON o.order_id = ol.order_id
JOIN {{ source('eco_essentials_landing', 'product') }} ps 
    ON ps.product_id = ol.product_id
LEFT JOIN {{ ref('e_dim_customer') }} cu 
    ON cu.customer_id = o.customer_id
JOIN {{ ref('e_dim_product') }} p 
    ON p.product_id = ol.product_id
JOIN {{ ref('e_dim_campaign') }} c 
    ON c.campaign_id = ol.campaign_id
JOIN {{ ref('e_dim_date') }} d 
    ON d.date = DATE(o.order_timestamp)