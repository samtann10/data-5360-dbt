{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
   )
}}

SELECT
   {{ dbt_utils.generate_surrogate_key(['product_id', 'product_type']) }} as product_key,
   product_id,
   product_type,
   product_name
FROM {{ source('eco_essential_landing', 'product') }}
