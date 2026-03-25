{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['emaileventid','eventtype']) }} as event_key,
    emaileventid,
    eventtype
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}




