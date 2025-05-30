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
        taker_buy_base, taker_buy_quote,
        row_number() over (partition by symbol, event_date order by event_date desc) as row_num
    from {{ ref('stg_crypto_daily_ohlcv') }}
)

select *
from source
where row_num = 1
