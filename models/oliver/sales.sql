{{ config(materialized="table", schema="dw_oliver") }}


select
    f.order_id,
    c.first_name,
    c.last_name,
    d.ddate,
    s.store_name,
    e.first_name as employee_fname,
    e.last_name as employee_lname
    p.product_name
    f.unit_price
from {{ ref("fact_sales") }} f

left join {{ ref("oliver_dim_customer") }} c on f.customer_key = c.customer_key

left join {{ ref("oliver_dim_store") }} s on f.store_key = s.store_key

left join {{ ref("oliver_dim_date") }} d on f.date_key = d.date_key

left join {{ ref("oliver_dim_product") }} p on f.product_key = p.product_key

left join {{ ref("oliver_dim_employee") }} e on f.employee_key = e.employee_key
