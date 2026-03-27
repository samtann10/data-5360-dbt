{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
   )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_first_name', 'customer_last_name']) }} as customer_key,
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_phone,
    customer_address,
    customer_city,
    customer_state,
    customer_zip,
    customer_email
FROM {{ source('eco_essentials_landing', 'customer') }}


