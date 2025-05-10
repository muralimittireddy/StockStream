-- models/agg_daily_ohlcv.sql
SELECT
  symbol,
  DATE(ts) AS trade_date,
  MIN(low) AS day_low,
  MAX(high) AS day_high,
  FIRST_VALUE(open) OVER (PARTITION BY symbol, DATE(ts) ORDER BY ts) AS day_open,
  LAST_VALUE(close) OVER (PARTITION BY symbol, DATE(ts) ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS day_close,
  SUM(volume) AS total_volume
FROM {{ ref('stg_ohlcv') }}
GROUP BY symbol, trade_date
