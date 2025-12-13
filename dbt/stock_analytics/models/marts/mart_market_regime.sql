{{ config(materialized='table') }}

with daily_stats as (
    select
        date,
        -- Offensive (Cyclical)
        avg(case when symbol in ('XLK', 'XLY', 'XLF') then rolling_beta end) as offensive_beta,
        -- Defensive (Safety)
        avg(case when symbol in ('XLU', 'XLP', 'XLV') then rolling_beta end) as defensive_beta,
        
        -- Bubble Metrics
        avg(rolling_beta) as avg_market_beta,
        max(case when symbol = 'XLK' then rolling_beta end) as tech_beta
    from {{ ref('mart_sector_volatility') }}
    group by 1
)

select
    date,
    offensive_beta,
    defensive_beta,
    (offensive_beta - defensive_beta) as sentiment_spread,
    
    -- THE BUBBLE GAUGE
    -- Use div0 function: Returns 0 if divide by zero (Standard Snowflake function)
    div0(tech_beta, avg_market_beta) as tech_bubble_ratio

from daily_stats
-- STRICT FILTER: Ensure we have valid numbers for the calculation
where tech_beta is not null 
  and avg_market_beta is not null
  and avg_market_beta != 0
order by 1