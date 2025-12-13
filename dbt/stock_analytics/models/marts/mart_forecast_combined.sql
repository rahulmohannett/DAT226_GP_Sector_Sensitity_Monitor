{{ config(materialized='table') }}

with source_actuals as (
    select
        symbol,
        sector_name,
        date,
        rolling_beta as actual_beta
    from {{ ref('mart_sector_volatility') }}
    where rolling_beta is not null
),

source_predictions as (
    select
        p.symbol,
        
        -- FIX: Handle SPY name here too
        case 
            when p.symbol = 'SPY' then 'S&P 500 Benchmark'
            else coalesce(m.sector_name, p.symbol)
        end as sector_name,
        
        p.date,
        p.forecast as forecast_beta,
        p.lower_bound,
        p.upper_bound
    from {{ source('analytics', 'sector_volatility_prediction') }} p
    left join {{ ref('sector_metadata') }} m
        on p.symbol = m.symbol
),

-- Find the "Seam" - the last valid historical date for each symbol
seam_date as (
    select 
        symbol, 
        max(date) as split_date
    from source_actuals
    group by 1
),

-- 1. History Slice
history_slice as (
    select
        a.symbol,
        a.sector_name,
        a.date,
        a.actual_beta,
        case 
            when a.date = s.split_date then a.actual_beta 
            else null 
        end as forecast_beta,
        null as lower_bound,
        null as upper_bound,
        'history' as type
    from source_actuals a
    left join seam_date s 
        on a.symbol = s.symbol
),

-- 2. Forecast Slice
forecast_slice as (
    select
        p.symbol,
        p.sector_name,
        p.date,
        null as actual_beta,
        p.forecast_beta,
        p.lower_bound,
        p.upper_bound,
        'forecast' as type
    from source_predictions p
    inner join seam_date s
        on p.symbol = s.symbol
    where p.date > s.split_date
)

select * from history_slice
union all
select * from forecast_slice
order by symbol, date