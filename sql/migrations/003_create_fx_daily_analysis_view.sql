CREATE OR REPLACE VIEW v_fx_daily_analysis AS
WITH candles_with_previous_close AS (
    SELECT
        source,
        symbol,
        event_time,
        open,
        high,
        low,
        close,
        LAG(close) OVER (
            PARTITION BY source, symbol
            ORDER BY event_time
        ) AS previous_close
    FROM fx_daily_bars
)
SELECT
    source,
    symbol,
    (event_time AT TIME ZONE 'UTC')::date AS data_utc,
    event_time,
    open,
    high,
    low,
    close,
    previous_close,
    ROUND(
        ((close - previous_close) / NULLIF(previous_close, 0) * 100)::numeric,
        4
    ) AS variacao_desde_fecho_anterior_pct
FROM candles_with_previous_close;
