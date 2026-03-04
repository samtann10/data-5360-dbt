{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

select
Employee_ID AS Employee_Key,
Employee_ID,
First_Name,
Last_Name,
Position,
Hire_Date,
Email,
Phone_Number
FROM {{ source('oliver_landing', 'employee') }}