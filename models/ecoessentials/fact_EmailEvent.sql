{{ config(
    materialized = 'table',
    schema = 'dw_eco_essentials'
) }}

SELECT
    et.time_key,
    ed.date_key,
    c.customer_key,
    cg.campaign_key,
    e.event_key,
    ei.emailid_key
FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }} me
LEFT JOIN {{ ref('e_dim_time') }} et
    ON et.time_string = TO_CHAR(TRY_TO_TIMESTAMP(me.eventtimestamp::VARCHAR), 'HH24:MI:SS')
LEFT JOIN {{ ref('e_dim_date') }} ed
    ON ed.date = TRY_TO_DATE(me.eventtimestamp::VARCHAR)
LEFT JOIN {{ ref('e_dim_customer') }} c
    ON TRIM(c.customer_id::VARCHAR) = TRIM(me.customerid::VARCHAR)
LEFT JOIN {{ ref('e_dim_campaign') }} cg
    ON TRIM(cg.campaign_id::VARCHAR) = TRIM(me.campaignid::VARCHAR)
LEFT JOIN {{ ref('e_dim_event') }} e
    ON UPPER(TRIM(e.eventtype::VARCHAR)) = UPPER(TRIM(me.eventtype::VARCHAR))
LEFT JOIN {{ ref('e_dim_emailid') }} ei
    ON TRIM(ei.emailid::VARCHAR) = TRIM(me.emailid::VARCHAR)
