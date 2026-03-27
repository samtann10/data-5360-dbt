{{ config(
    materialized = 'table',
    schema = 'dw_eco_essentials'
) }}

WITH combined_times AS (
    SELECT eventtimestamp AS ts
    FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}
    WHERE eventtimestamp IS NOT NULL

    UNION

    SELECT sendtimestamp AS ts
    FROM {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}
    WHERE sendtimestamp IS NOT NULL
),

time_data AS (
    SELECT DISTINCT
        DATE_PART(hour, ts) AS hour,
        DATE_PART(minute, ts) AS minute,
        DATE_PART(second, ts) AS second,
        TO_CHAR(ts, 'HH24:MI:SS') AS time_string
    FROM combined_times
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['hour','minute','second']) }} AS time_key,
    time_string,
    hour,
    minute,
    second
FROM time_data
ORDER BY hour, minute, second