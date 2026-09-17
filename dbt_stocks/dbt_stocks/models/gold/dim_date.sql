SELECT DISTINCT
    CAST(market_timestamp AS DATE) AS trade_date,
    YEAR(CAST(market_timestamp AS DATE)) AS year,
    MONTH(CAST(market_timestamp AS DATE)) AS month,
    DAY(CAST(market_timestamp AS DATE)) AS day
FROM {{ ref('silver_clean_stock_quotes') }}
WHERE market_timestamp IS NOT NULL