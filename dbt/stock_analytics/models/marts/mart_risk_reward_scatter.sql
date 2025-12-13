{{ config(materialized='table') }}

with daily_prices as (
    select 
        symbol,
        try_to_date(date::string) as clean_date,
        close
    from {{ ref('stg_market_data') }}
),

rolling_returns as (
    select
        symbol,
        clean_date as date,
        close as current_price,
        lag(close, 90) over (partition by symbol order by clean_date) as price_90_days_ago
    from daily_prices
),

risk_data as (
    select symbol, date, rolling_beta 
    from {{ ref('mart_sector_volatility') }}
),

metadata as (
    select * from {{ ref('sector_metadata') }}
),

combined as (
    select
        r.symbol,
        r.date,
        
        -- FIX 1: Handle SPY explicitly since it's not in the Seed
        case 
            when r.symbol = 'SPY' then 'S&P 500 Benchmark'
            -- FIX 2: Fallback to Symbol if seed join fails (prevents NULL errors)
            else coalesce(m.sector_name, r.symbol) 
        end as sector_name,
        
        m.approx_weight_pct,
        
        case 
            when r.symbol = 'SPY' then 1.0
            else coalesce(v.rolling_beta, 0)
        end as avg_risk_beta,
        
        (r.current_price - r.price_90_days_ago) / nullif(r.price_90_days_ago, 0) * 100 as total_return_pct

    from rolling_returns r
    left join risk_data v 
        on r.symbol = v.symbol and r.date = v.date
    left join metadata m 
        on r.symbol = m.symbol
    
    where r.price_90_days_ago is not null
)

select
    *,
    case 
        when symbol = 'SPY' then 'Benchmark'
        when total_return_pct > 0 and avg_risk_beta < 1.0 then 'Safety (Buy)'
        when total_return_pct > 0 and avg_risk_beta >= 1.0 then 'Momentum (Ride)'
        when total_return_pct <= 0 and avg_risk_beta >= 1.0 then 'Toxic (Sell)'
        else 'Stagnant (Avoid)'
    end as investment_type
from combined
order by date desc, symbol