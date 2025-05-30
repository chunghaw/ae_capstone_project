-- models/staging/stg_crypto_daily_ohlcv.sql
{{ config(materialized='table') }}

select
  symbol,
  event_date,
  open,
  high,
  low,
  close,
  volume,
  quote_asset_volume,
  number_of_trades,
  taker_buy_base,
  taker_buy_quote
from {{ source('chunghaw','crypto_daily_ohlcv') }}
