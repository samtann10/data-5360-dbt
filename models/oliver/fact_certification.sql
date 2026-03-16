{{ config(
    materialized = 'table',
    schema = 'oliver_dw_source'
    )
}}

select
    d.datekey,
    em.employee_key,
    e.certification_name,
    e.certification_cost
from {{ ref('stg_employee_certifications')}} e
    inner join {{ ref('oliver_dim_date')}} d ON d.ddate = e.certification_awarded_date
    inner join {{ ref('oliver_dim_employee')}} em ON em.employee_id = e.employee_id