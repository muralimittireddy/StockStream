-- models/top_movers.sql

{{ config(
    materialized='incremental',
    unique_key=['symbol', 'trade_day', 'category'],
    partition_by={"field": "trade_day", "data_type": "date"},
    cluster_by=["category"]
) }}

WITH daily_prices AS (
    SELECT
        symbol,
        category,
        DATE(ts) AS trade_day,
        FIRST_VALUE(open) OVER (
            PARTITION BY symbol, DATE(ts)
            ORDER BY ts
        ) AS open_price,
        LAST_VALUE(close) OVER (
            PARTITION BY symbol, DATE(ts)
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close_price
    FROM {{ ref('stg_ohlcv') }}
    {% if is_incremental() %}
        WHERE DATE(ts) >= CURRENT_DATE() - INTERVAL 1 DAY
    {% endif %}
),

deduplicated AS (
    SELECT DISTINCT symbol, category, trade_day, open_price, close_price,
        SAFE_DIVIDE(close_price - open_price, open_price) AS return_pct
    FROM daily_prices
),

filtered AS (
    SELECT *
    FROM deduplicated
    WHERE (
        category = 'crypto_currency'
        OR (
            category IN ('stock', 'etf', 'currency', 'indice')
            AND EXTRACT(DAYOFWEEK FROM trade_day) BETWEEN 2 AND 6
        )
    )
),

ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY trade_day, category ORDER BY return_pct DESC) AS gain_rank,
        RANK() OVER (PARTITION BY trade_day, category ORDER BY return_pct ASC) AS loss_rank
    FROM filtered
)

SELECT *
FROM ranked
WHERE gain_rank <= 5 OR loss_rank <= 5
