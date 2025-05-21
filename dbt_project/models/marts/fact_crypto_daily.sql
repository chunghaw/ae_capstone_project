{{ config(
    materialized='incremental',
    unique_key=['symbol', 'event_date']
) }}

with source as (
    select
        symbol,
        event_date,
        open, high, low, close,
        volume, quote_asset_volume,
        number_of_trades,
        taker_buy_base, taker_buy_quote
    from {{ ref('stg_crpto_daily_ohlcv') }}
)

select * from source