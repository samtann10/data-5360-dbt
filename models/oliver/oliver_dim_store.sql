{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

select
Store_ID AS Store_Key,
Store_ID,
Store_Name,
Street,
City,
State
FROM {{ source('oliver_landing', 'store') }}