SELECT
    s.symbol,
    CAST(s.market_timestamp AS DATE) AS trade_date,
    s.current_price,
    s.change_amount,
    s.change_percent,
    s.day_high,
    s.day_low,
    s.day_open,
    s.prev_close,
    s.market_timestamp,
    s.fetched_at
FROM {{ ref('silver_clean_stock_quotes') }} s
WHERE s.current_price IS NOT NULL