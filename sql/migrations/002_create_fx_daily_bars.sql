CREATE TABLE IF NOT EXISTS fx_daily_bars (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'tiingo'
        CHECK (source = 'tiingo'),
    symbol VARCHAR(6) NOT NULL
        CHECK (symbol ~ '^[A-Z]{6}$'),
    event_time TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL CHECK (open > 0),
    high NUMERIC NOT NULL CHECK (high > 0),
    low NUMERIC NOT NULL CHECK (low > 0),
    close NUMERIC NOT NULL CHECK (close > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fx_daily_bars_ohlc_consistent
        CHECK (
            high >= GREATEST(open, close, low)
            AND low <= LEAST(open, close, high)
        ),
    CONSTRAINT fx_daily_bars_unique_day
        UNIQUE (source, symbol, event_time)
);
