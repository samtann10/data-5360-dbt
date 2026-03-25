{{ config(materialized='table', schema='dw_eco_essentials') }}

SELECT 
    t.time_key,
    d.date_key,
    cu.customer_key.
    c.campaign_key,
    e.event_key,
    em.emailid_key
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }} e
JOIN {{ ref('e_dim_date') }} d 
JOIN {{ ref('e_dim_customer') }} cu
JOIN {{ ref('e_dim_campaign') }} c 
JOIN {{ ref('e_dim_event')}} e
JOIN {{ ref('e_emailid')}} em
