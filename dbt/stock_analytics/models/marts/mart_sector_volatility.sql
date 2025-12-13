{{ config(materialized='table') }}

with returns as ( select * from {{ ref('int_daily_returns') }} ),

benchmark as (
    select date, daily_return as market_return
    from returns where symbol = 'SPY'
),

sectors as (
    select symbol, date, daily_return as sector_return
    from returns where symbol != 'SPY'
),

joined as (
    select
        s.symbol, s.date, s.sector_return, b.market_return
    from sectors s
    inner join benchmark b on s.date = b.date
),

rolling_stats as (
    select
        symbol, date, market_return, sector_return,
        count(*) over (partition by symbol order by date rows between 90 preceding and current row) as n,
        sum(market_return) over (partition by symbol order by date rows between 90 preceding and current row) as sum_m,
        sum(sector_return) over (partition by symbol order by date rows between 90 preceding and current row) as sum_s,
        sum(market_return * sector_return) over (partition by symbol order by date rows between 90 preceding and current row) as sum_ms,
        sum(market_return * market_return) over (partition by symbol order by date rows between 90 preceding and current row) as sum_mm
    from joined
)

select
    r.symbol,
    m.sector_name,
    r.date,
    
    -- Volatility Metrics
    (sum_ms - (sum_m * sum_s / nullif(n, 0))) / nullif(n - 1, 0) as rolling_covariance,
    (sum_mm - (sum_m * sum_m / nullif(n, 0))) / nullif(n - 1, 0) as rolling_market_variance,
    div0(
        (sum_ms - (sum_m * sum_s / nullif(n, 0))),
        (sum_mm - (sum_m * sum_m / nullif(n, 0)))
    ) as rolling_beta,
    
    -- New Metric: Weighted Impact (Risk Contribution)
    -- (Beta * Weight) / 100
    (div0(
        (sum_ms - (sum_m * sum_s / nullif(n, 0))),
        (sum_mm - (sum_m * sum_m / nullif(n, 0)))
    ) * m.approx_weight_pct) / 100 as weighted_impact

from rolling_stats r
-- Join to the seed file
left join {{ ref('sector_metadata') }} m 
    on r.symbol = m.symbol
order by r.symbol, r.date