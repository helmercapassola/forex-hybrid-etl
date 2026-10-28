-- Diagnostico somente de leitura para a qualidade das tabelas Forex.
-- Lacunas em dias uteis sao sinalizadas para revisao; feriados nao sao
-- classificados automaticamente porque nao existe um calendario de mercado.
WITH check_catalog(check_name, severity, description) AS (
    VALUES
        (
            'daily_invalid_ohlc',
            'ERROR',
            'Velas diarias com precos nao positivos ou OHLC incoerente.'
        ),
        (
            'daily_duplicate_keys',
            'ERROR',
            'Chaves repetidas em fx_daily_bars.'
        ),
        (
            'quote_invalid_bid_ask',
            'ERROR',
            'Cotasoes sem bid/ask valido, preco nao positivo ou ask abaixo do bid.'
        ),
        (
            'quote_duplicate_keys',
            'ERROR',
            'Chaves repetidas em fx_quotes.'
        ),
        (
            'daily_calendar_gaps',
            'REVIEW',
            'Intervalos com dias uteis sem vela; confirmar fins de semana e feriados.'
        )
),
findings(check_name, details) AS (
    SELECT
        'daily_invalid_ohlc',
        jsonb_build_object(
            'source', source,
            'symbol', symbol,
            'event_time', event_time,
            'open', open,
            'high', high,
            'low', low,
            'close', close
        )
    FROM fx_daily_bars
    WHERE open <= 0
       OR high <= 0
       OR low <= 0
       OR close <= 0
       OR high < GREATEST(open, close, low)
       OR low > LEAST(open, close, high)

    UNION ALL

    SELECT
        'daily_duplicate_keys',
        jsonb_build_object(
            'source', source,
            'symbol', symbol,
            'event_time', event_time,
            'copies', COUNT(*)
        )
    FROM fx_daily_bars
    GROUP BY source, symbol, event_time
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'quote_invalid_bid_ask',
        jsonb_build_object(
            'source', source,
            'symbol', symbol,
            'event_time', event_time,
            'bid_price', bid_price,
            'ask_price', ask_price
        )
    FROM fx_quotes
    WHERE bid_price IS NULL
       OR ask_price IS NULL
       OR bid_price <= 0
       OR ask_price <= 0
       OR ask_price < bid_price

    UNION ALL

    SELECT
        'quote_duplicate_keys',
        jsonb_build_object(
            'source', source,
            'symbol', symbol,
            'event_time', event_time,
            'raw_data', raw_data,
            'copies', COUNT(*)
        )
    FROM fx_quotes
    GROUP BY source, symbol, event_time, raw_data
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'daily_calendar_gaps',
        jsonb_build_object(
            'source', source,
            'symbol', symbol,
            'previous_date_utc', previous_date_utc,
            'current_date_utc', current_date_utc,
            'missing_calendar_days', current_date_utc - previous_date_utc - 1,
            'missing_weekdays_utc', missing_weekdays_utc
        )
    FROM (
        SELECT
            source,
            symbol,
            previous_date_utc,
            current_date_utc,
            ARRAY(
                SELECT missing_day::date
                FROM generate_series(
                    previous_date_utc + 1,
                    current_date_utc - 1,
                    INTERVAL '1 day'
                ) AS missing_day
                WHERE EXTRACT(ISODOW FROM missing_day) BETWEEN 1 AND 5
                ORDER BY missing_day
            ) AS missing_weekdays_utc
        FROM (
            SELECT
                source,
                symbol,
                (LAG(event_time) OVER (
                    PARTITION BY source, symbol
                    ORDER BY event_time
                ) AT TIME ZONE 'UTC')::date AS previous_date_utc,
                (event_time AT TIME ZONE 'UTC')::date AS current_date_utc
            FROM fx_daily_bars
        ) AS ordered_daily_bars
        WHERE previous_date_utc IS NOT NULL
          AND current_date_utc - previous_date_utc > 1
    ) AS daily_gaps
),
check_totals AS (
    SELECT check_name, COUNT(*) AS finding_count
    FROM findings
    GROUP BY check_name
),
ranked_examples AS (
    SELECT
        check_name,
        details,
        ROW_NUMBER() OVER (
            PARTITION BY check_name
            ORDER BY details::text
        ) AS example_number
    FROM findings
),
check_examples AS (
    SELECT
        check_name,
        JSONB_AGG(details ORDER BY example_number) AS examples
    FROM ranked_examples
    WHERE example_number <= 10
    GROUP BY check_name
)
SELECT
    catalog.check_name,
    catalog.severity,
    catalog.description,
    COALESCE(totals.finding_count, 0) AS finding_count,
    COALESCE(examples.examples, '[]'::jsonb) AS examples
FROM check_catalog AS catalog
LEFT JOIN check_totals AS totals USING (check_name)
LEFT JOIN check_examples AS examples USING (check_name)
ORDER BY
    CASE catalog.severity WHEN 'ERROR' THEN 1 ELSE 2 END,
    catalog.check_name;
