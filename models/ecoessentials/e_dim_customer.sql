{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
   )
}}

WITH customers AS (
    SELECT DISTINCT
            {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'customer_first_name',
            'customer_last_name'
        ]) }} AS customer_key,
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
),

subscribers AS (
    SELECT DISTINCT
            {{ dbt_utils.generate_surrogate_key([
            'subscriberemail',
            'subscriberfirstname',
            'subscriberlastname'
        ]) }} AS customer_key,
        NULL AS customer_id,
        subscriberfirstname AS customer_first_name,
        subscriberlastname AS customer_last_name,
        NULL AS customer_phone,
        NULL AS customer_address,
        NULL AS customer_city,
        NULL AS customer_state,
        NULL AS customer_zip,
        subscriberemail AS customer_email
    FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}
    WHERE customerid = 'NULL'
)
SELECT * FROM customers
UNION ALL
SELECT * FROM subscribers