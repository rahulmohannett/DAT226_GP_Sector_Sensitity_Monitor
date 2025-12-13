{{ config(materialized='view') }}

select distinct
    upper(symbol)                        as symbol,
    cast(date as date)                   as date,
    cast(open as float)                  as open,
    cast(high as float)                  as high,
    cast(low as float)                   as low,
    cast(close as float)                 as close,
    cast(volume as float)                as volume,
    -- Primary Key for testing
    concat(upper(symbol), '|', to_varchar(cast(date as date))) as pk_symbol_date
from {{ source('raw','MARKET_DATA') }}
where date is not null