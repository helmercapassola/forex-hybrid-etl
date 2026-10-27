CREATE OR REPLACE VIEW v_fx_quote_spread AS
SELECT
    source,
    symbol,
    event_time,
    received_at,
    bid_price,
    mid_price,
    ask_price,
    CASE
        WHEN bid_price IS NOT NULL
         AND ask_price IS NOT NULL
         AND ask_price >= bid_price
        THEN ask_price - bid_price
    END AS spread,
    CASE
        WHEN bid_price IS NOT NULL
         AND ask_price IS NOT NULL
         AND ask_price >= bid_price
         AND mid_price IS NOT NULL
         AND mid_price <> 0
        THEN ROUND(
            ((ask_price - bid_price) / mid_price * 100)::numeric,
            6
        )
    END AS spread_pct_of_mid,
    bid_price IS NOT NULL
        AND ask_price IS NOT NULL
        AND ask_price >= bid_price AS is_valid_spread
FROM fx_quotes;
