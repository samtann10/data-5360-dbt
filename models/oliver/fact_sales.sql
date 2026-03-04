{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
c.Customer_Key,
p.Product_Key,
d.DateKey,
e.Employee_Key,
s.Store_Key,
ol.Unit_Price,
ol.Quantity,
ol.Order_ID
FROM {{ source('oliver_landing', 'orders') }} o
INNER JOIN {{ source('oliver_landing', 'orderline') }} ol ON o.order_id = ol.order_id
INNER JOIN {{ ref('oliver_dim_customer') }} c ON o.customer_id = c.customer_id
INNER JOIN {{ ref('oliver_dim_date') }} d ON d.ddate = o.order_date
INNER JOIN {{ ref('oliver_dim_employee') }} e ON e.employee_id = o.employee_id
INNER JOIN {{ ref('oliver_dim_product') }} p ON p.product_id = ol.product_id
INNER JOIN {{ ref('oliver_dim_store')}} s ON s.store_id = o.store_id
