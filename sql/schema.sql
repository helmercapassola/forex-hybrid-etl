CREATE TABLE IF NOT EXISTS fx_quotes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'tiingo'
        CHECK (source = 'tiingo'),
    symbol VARCHAR(6) NOT NULL
        CHECK (symbol ~ '^[A-Z]{6}$'),
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    bid_size NUMERIC,
    bid_price NUMERIC,
    mid_price NUMERIC,
    ask_size NUMERIC,
    ask_price NUMERIC,
    raw_data JSONB NOT NULL
        CHECK (jsonb_typeof(raw_data) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fx_quotes_unique_tick
        UNIQUE (source, symbol, event_time, raw_data)
);
