{{ config(materialized='table', schema='dw_eco_essentials') }}

SELECT 
    t.time_key,
    d.date_key,
    cu.customer_key,
    c.campaign_key,
    e.event_key,
    em.emailid_key
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }} me
JOIN {{ ref('e_dim_date') }} d 
    ON d.date = CAST(me.eventtimestamp AS DATE)
JOIN {{ ref('e_dim_time') }} t
    ON t.time_key = TO_CHAR(me.eventtimestamp, 'HH24MISS')
--could be duplicates, but there are missing customerids in the source table
JOIN {{ ref('e_dim_customer') }} cu 
    ON cu.customer_first_name = me.subscriberfirstname
   AND cu.customer_last_name = me.subscriberlastname
   AND cu.customer_email = me.subscriberemail
JOIN {{ ref('e_dim_campaign') }} c 
    ON c.campaign_id = me.campaignid
JOIN {{ ref('e_dim_event') }} e 
    ON e.eventtype = me.eventtype
JOIN {{ ref('e_dim_emailid') }} em 
    ON em.emailid = me.emailid