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
    FROM {{ ref('crypto_stg_ohlcv') }}
    {% if is_incremental() %}
        WHERE DATE(ts) >= CURRENT_DATE() - INTERVAL 1 DAY
    {% endif %}
),

-- Deduplicate to 1 row per symbol, trade_day, category
deduplicated AS (
    SELECT DISTINCT
        symbol,
        category,
        trade_day,
        open_price,
        close_price,
        SAFE_DIVIDE(close_price - open_price, open_price) AS return_pct
    FROM daily_prices
    WHERE open_price IS NOT NULL AND close_price IS NOT NULL AND open_price > 0
),

-- Only include crypto always, others only Mon-Fri (not weekends)
filtered AS (
    SELECT *
    FROM deduplicated
    WHERE
        category = 'crypto_currency' 
),

-- Rank gainers and losers per category per trade_day
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY trade_day, category ORDER BY return_pct DESC) AS gain_rank,
        RANK() OVER (PARTITION BY trade_day, category ORDER BY return_pct ASC) AS loss_rank
    FROM filtered
)

-- Output top 5 gainers/losers per category, per day
SELECT
    symbol,
    category,
    trade_day,
    open_price,
    close_price,
    return_pct,
    gain_rank,
    loss_rank
FROM ranked
WHERE gain_rank <= 5 OR loss_rank <= 5
