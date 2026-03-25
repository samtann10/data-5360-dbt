{{ config(
   materialized = 'table',
   schema = 'dw_eco_essential'
   )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }}
)


SELECT
   {{ dbt_utils.generate_surrogate_key(['date_day', 'year_number']) }} as date_key,
   date_day as date,
   day_of_week as day,
   month_of_year as month,
   year_number as year
from cte_date
