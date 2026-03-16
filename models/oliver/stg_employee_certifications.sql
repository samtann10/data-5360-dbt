{{ config (
    materialized = 'table',
    schema = 'oliver_dw_source'
) }}

SELECT
    employee_id,
    first_name,
    last_name,
    PARSE_JSON(certification_json):certification_name::VARCHAR(50) AS certification_name,
    PARSE_JSON(certification_json):certification_cost::INT AS certification_cost,
    PARSE_JSON(certification_json):certification_awarded_date::DATE AS certification_awarded_date
FROM {{ source('oliver_landing', 'employee_certifications') }}