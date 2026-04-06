{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
)}}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['emailid','emailname']) }} as emailid_key,
    emailid,
    emailname
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}