-- models/stg_ohlcv.sql
WITH raw AS (
  SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    symbol,
    CAST(open AS FLOAT64) AS open,
    CAST(high AS FLOAT64) AS high,
    CAST(low AS FLOAT64) AS low,
    CAST(close AS FLOAT64) AS close,
    CAST(volume AS FLOAT64) AS volume
  FROM `solarcropsanalysis-454507.ohlcv_dataset.ohlcv_table`
)

SELECT * FROM raw
