-- stg_weather.sql
-- Cleans and standardises raw hourly weather records from ClickHouse

WITH source AS (

    SELECT *
    FROM {{ source('weather_analytics', 'raw_weather') }}

),

cleaned AS (

    SELECT
        -- identifiers
        city,
        country,
        latitude,
        longitude,

        -- time
        recorded_at,
        toDate(recorded_at)            AS record_date,
        toHour(recorded_at)            AS record_hour,
        toDayOfWeek(recorded_at)       AS day_of_week,     -- 1=Mon … 7=Sun
        toMonth(recorded_at)           AS record_month,
        toYear(recorded_at)            AS record_year,

        -- weather metrics (nulls kept; downstream models decide how to handle)
        temperature_c,
        humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        weather_code,

        -- weather description based on WMO code
        CASE
            WHEN weather_code = 0                  THEN 'Clear Sky'
            WHEN weather_code IN (1, 2, 3)         THEN 'Partly Cloudy'
            WHEN weather_code IN (45, 48)           THEN 'Foggy'
            WHEN weather_code BETWEEN 51 AND 67    THEN 'Drizzle / Rain'
            WHEN weather_code BETWEEN 71 AND 77    THEN 'Snow'
            WHEN weather_code BETWEEN 80 AND 82    THEN 'Rain Showers'
            WHEN weather_code BETWEEN 95 AND 99    THEN 'Thunderstorm'
            ELSE                                        'Unknown'
        END AS weather_description,

        ingested_at

    FROM source
    WHERE recorded_at IS NOT NULL
      AND city        IS NOT NULL

)

SELECT * FROM cleaned