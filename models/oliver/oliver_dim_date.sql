{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

select
date_day AS DateKey,
date_day AS DDate,
day_of_week AS DayofWeek,
month_of_year AS Month,
quarter_of_year AS Quarter,
year_number AS Year
FROM cte_date