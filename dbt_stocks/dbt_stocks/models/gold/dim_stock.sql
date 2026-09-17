SELECT DISTINCT
    symbol
FROM {{ ref('silver_clean_stock_quotes') }}
WHERE symbol IS NOT NULL