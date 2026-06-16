-- mart_daily_weather_summary.sql
-- Analytics-ready daily aggregates per city — optimised for ClickHouse

WITH staged AS (

    SELECT * FROM {{ ref('stg_weather') }}

),

daily_agg AS (

    SELECT
        city,
        country,
        record_date,
        record_month,
        record_year,
        day_of_week,
        -- temperature
        round(avg(temperature_c),  2)  AS avg_temp_c,
        round(min(temperature_c),  2)  AS min_temp_c,
        round(max(temperature_c),  2)  AS max_temp_c,
        -- humidity
        round(avg(humidity_pct),   2)  AS avg_humidity_pct,

        -- precipitation
        round(sum(precipitation_mm), 3) AS total_precipitation_mm,
        -- wind
        round(avg(wind_speed_kmh), 2)  AS avg_wind_speed_kmh,
        round(max(wind_speed_kmh), 2)  AS max_wind_speed_kmh,
        -- dominant weather condition for the day
        topK(1)(weather_description)[1] AS dominant_weather,

        count(*)  AS hourly_records_count
    FROM staged
    GROUP BY city, country, record_date, record_month, record_year, day_of_week

)

SELECT
    *,
    -- derived comfort index (simple Heat Index proxy)
    round(
        avg_temp_c
        + (0.33 * (avg_humidity_pct / 100) * 6.105
           * exp((17.27 * avg_temp_c) / (237.7 + avg_temp_c)))
        - 4.0,
        2
    ) AS heat_index_approx
FROM daily_agg
ORDER BY city, record_date