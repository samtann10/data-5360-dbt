{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}

with db_source as (
select
customerid,
firstname,
lastname,
dob,
address,
city,
state,
zipcode
FROM {{ source('insurance_landing', 'customers') }}
), cs_interactions_source as (
    select distinct
        customer_first_name,
        customer_last_name,
        customer_email
    from {{ ref('stg_customer_service_interactions')}}
), final as (
    select
        d.customerid,
        coalesce(d.firstname, c.customer_first_name) as firstname,
        coalesce(d.lastname, c.customer_last_name) as lastname,
        d.dob,
        d.address,
        d.city,
        d.state,
        d.zipcode,
        c.customer_email
    from db_source d full join cs_interactions_source c 
        on d.firstname = c.customer_first_name and d.lastname = c.customer_last_name
)

select
    {{ dbt_utils.generate_surrogate_key(['firstname', 'lastname'])}} as customer_key,
    *
from final