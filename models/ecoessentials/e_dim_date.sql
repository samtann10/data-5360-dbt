{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
   )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("2025-01-01", "2025-12-31") }}
)

SELECT
   {{ dbt_utils.generate_surrogate_key(['date_day', 'year_number']) }} as date_key,
    date_day as date,
    year_number as year,
    month_of_year as month,
    day_of_week as day
from cte_date



