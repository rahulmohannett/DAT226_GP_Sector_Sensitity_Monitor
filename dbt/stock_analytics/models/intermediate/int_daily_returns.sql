{{ config(materialized='view') }}

with base as (
    select
        symbol,
        date,
        close
    from {{ ref('stg_market_data') }}
),

with_lag as (
    select
        *,
        -- Get previous day's closing price for the same symbol
        lag(close) over (partition by symbol order by date) as prev_close
    from base
)

select
    symbol,
    date,
    close,
    -- Simple Return = (Price - Prev) / Prev
    -- Using nullif to avoid divide-by-zero errors
    (close - prev_close) / nullif(prev_close, 0) as daily_return
from with_lag
where prev_close is not null