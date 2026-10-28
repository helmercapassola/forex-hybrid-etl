CREATE OR REPLACE VIEW v_fx_monthly_ohlc AS
WITH daily_bars_with_month AS (
    SELECT
        source,
        symbol,
        event_time,
        open,
        high,
        low,
        close,
        date_trunc('month', event_time AT TIME ZONE 'UTC')::date
            AS month_start_utc
    FROM fx_daily_bars
)
SELECT
    source,
    symbol,
    month_start_utc,
    MIN((event_time AT TIME ZONE 'UTC')::date) AS first_bar_date_utc,
    MAX((event_time AT TIME ZONE 'UTC')::date) AS last_bar_date_utc,
    (ARRAY_AGG(open ORDER BY event_time ASC))[1] AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    (ARRAY_AGG(close ORDER BY event_time DESC))[1] AS close,
    COUNT(*) AS daily_bar_count
FROM daily_bars_with_month
GROUP BY source, symbol, month_start_utc;
