{{ config(materialized='view') }}

select 
  symbol,
  event_date,
  open_time,
  close_time,
  open::float,
  high::float,
  low::float,
  close::float,
  volume::float,
  quote_asset_volume::float,
  number_of_trades::int,
  taker_buy_base::float,
  taker_buy_quote::float
from {{ source('binance', 'crypto_daily_ohlcv') }}