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
    ON (
        -- Priority 1: ID Match (only if it's not the string 'NULL')
        UPPER(TRIM(me.customerid)) != 'NULL' 
        AND TRIM(c.customer_id::VARCHAR) = TRIM(me.customerid::VARCHAR)
    )
     -- Priority 2: Email Match for those 'NULL' string rows
    OR (UPPER(TRIM(me.customerid)) = 'NULL'
        AND REPLACE(LOWER(TRIM(c.customer_email)), '''', '') = 
            REPLACE(LOWER(TRIM(me.subscriberemail)), '''', ''))
LEFT JOIN {{ ref('e_dim_campaign') }} cg
    ON TRIM(cg.campaign_id::VARCHAR) = TRIM(me.campaignid::VARCHAR)
LEFT JOIN {{ ref('e_dim_event') }} e
    ON UPPER(TRIM(e.eventtype::VARCHAR)) = UPPER(TRIM(me.eventtype::VARCHAR))
LEFT JOIN {{ ref('e_dim_email') }} ei
    ON TRIM(ei.emailid::VARCHAR) = TRIM(me.emailid::VARCHAR)