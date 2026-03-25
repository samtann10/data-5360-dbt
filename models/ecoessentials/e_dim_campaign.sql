{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
   )
}}

SELECT
   {{ dbt_utils.generate_surrogate_key(['campaign_id', 'campaign_name']) }} as campaign_key,
   campaign_id,
   campaign_name,
   campaign_discount
FROM {{ source('eco_essential_landing', 'promotional_campaign') }}