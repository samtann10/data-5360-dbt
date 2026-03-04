{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

select
Product_ID AS Product_Key,
Product_ID,
Product_Name,
Description
FROM {{ source('oliver_landing', 'product') }}