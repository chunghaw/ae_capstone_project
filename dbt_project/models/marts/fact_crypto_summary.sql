{{ config(materialized='table') }}

with base as (
  select * from {{ ref('stg_binance_klines') }}
)

select
  year(event_date) as event_year,
  month(event_date) as event_month,
  symbol,
  avg(close)              as avg_close,
  sum(volume)             as total_volume
from base
group by  symbol, year(event_date), month(event_date)
order by event_year, event_month
