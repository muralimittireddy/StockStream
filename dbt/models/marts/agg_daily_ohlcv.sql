-- models/agg_daily_ohlcv.sql

WITH base AS (
  SELECT
    symbol,
    DATE(ts) AS trade_date,
    ts,
    open,
    close,
    high,
    low,
    volume,
    FIRST_VALUE(open) OVER (PARTITION BY symbol, DATE(ts) ORDER BY ts) AS day_open,
    LAST_VALUE(close) OVER (
      PARTITION BY symbol, DATE(ts) ORDER BY ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS day_close
  FROM {{ ref('stg_ohlcv') }}
)

SELECT
  symbol,
  trade_date,
  MIN(low) AS day_low,
  MAX(high) AS day_high,
  MAX(day_open) AS day_open,
  MAX(day_close) AS day_close,
  SUM(volume) AS total_volume
FROM base
GROUP BY symbol, trade_date
