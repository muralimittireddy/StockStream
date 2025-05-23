-- models/staging/crypto_stg_ohlcv.sql

WITH raw AS (
  SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    symbol,
    CAST(open AS FLOAT64) AS open,
    CAST(high AS FLOAT64) AS high,
    CAST(low AS FLOAT64) AS low,
    CAST(close AS FLOAT64) AS close,
    CAST(volume AS FLOAT64) AS volume,
    category  -- assumed to exist in your raw table
  FROM `solarcropsanalysis-454507.ohlcv_dataset.crypto_ohlcv_table`
)

SELECT * FROM raw
