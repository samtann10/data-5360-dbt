{{ config(
    materialized = 'table',
    schema = 'dw_eco_essentials'
    )
}}

SELECT
    distinct {{ dbt_utils.generate_surrogate_key(['eventtype']) }} as event_key,
    eventtype
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}



