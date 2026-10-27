ALTER TABLE fx_quotes
    ADD COLUMN IF NOT EXISTS bid_size NUMERIC,
    ADD COLUMN IF NOT EXISTS bid_price NUMERIC,
    ADD COLUMN IF NOT EXISTS mid_price NUMERIC,
    ADD COLUMN IF NOT EXISTS ask_size NUMERIC,
    ADD COLUMN IF NOT EXISTS ask_price NUMERIC;

UPDATE fx_quotes
SET
    bid_size = CASE
        WHEN jsonb_typeof(raw_data->3) = 'number' THEN (raw_data->>3)::numeric
    END,
    bid_price = CASE
        WHEN jsonb_typeof(raw_data->4) = 'number' THEN (raw_data->>4)::numeric
    END,
    mid_price = CASE
        WHEN jsonb_typeof(raw_data->5) = 'number' THEN (raw_data->>5)::numeric
    END,
    ask_size = CASE
        WHEN jsonb_typeof(raw_data->6) = 'number' THEN (raw_data->>6)::numeric
    END,
    ask_price = CASE
        WHEN jsonb_typeof(raw_data->7) = 'number' THEN (raw_data->>7)::numeric
    END;
