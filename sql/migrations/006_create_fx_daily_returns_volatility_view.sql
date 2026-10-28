CREATE OR REPLACE VIEW v_fx_daily_returns_volatility AS
WITH returns_with_window_stats AS (
    SELECT
        source,
        symbol,
        data_utc,
        event_time,
        open,
        high,
        low,
        close,
        previous_close,
        variacao_desde_fecho_anterior_pct AS daily_return_pct,
        COUNT(variacao_desde_fecho_anterior_pct) OVER (
            PARTITION BY source, symbol
            ORDER BY event_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS returns_in_5_observation_window,
        STDDEV_SAMP(variacao_desde_fecho_anterior_pct) OVER (
            PARTITION BY source, symbol
            ORDER BY event_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS sample_volatility_5_returns_pct
    FROM v_fx_daily_analysis
)
SELECT
    source,
    symbol,
    data_utc,
    event_time,
    open,
    high,
    low,
    close,
    previous_close,
    daily_return_pct,
    returns_in_5_observation_window,
    CASE
        WHEN returns_in_5_observation_window = 5
        THEN ROUND(sample_volatility_5_returns_pct, 6)
    END AS volatility_5_returns_pct
FROM returns_with_window_stats;
