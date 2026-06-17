-- mart_weekly_weather_trends.sql
-- ISO-week aggregates per city with week-over-week deltas.
-- Useful for trend analysis and anomaly detection baselines.

WITH daily AS (

    SELECT * FROM {{ ref('mart_daily_weather_summary') }}

),

weekly_agg AS (

    SELECT
        city,
        country,
        toISOYear(record_date)                       AS iso_year,
        toISOWeek(record_date)                       AS iso_week,
        -- anchor the week to its Monday
        toMonday(record_date)                        AS week_start_date,

        -- temperature
        round(avg(avg_temp_c),    2)                 AS avg_temp_c,
        round(min(min_temp_c),    2)                 AS min_temp_c,
        round(max(max_temp_c),    2)                 AS max_temp_c,

        -- precipitation
        round(sum(total_precipitation_mm), 3)        AS total_precipitation_mm,
        countIf(total_precipitation_mm > 0)          AS rainy_days,

        -- wind
        round(avg(avg_wind_speed_kmh), 2)            AS avg_wind_speed_kmh,

        -- humidity
        round(avg(avg_humidity_pct),  2)             AS avg_humidity_pct,

        -- dominant condition for the week
        topK(1)(dominant_weather)[1]                 AS dominant_weather,

        count(*)                                     AS days_in_week

    FROM daily
    GROUP BY city, country, iso_year, iso_week, week_start_date

),

with_deltas AS (

    SELECT
        *,
        -- week-over-week temperature change
        round(
            avg_temp_c
            - lagInFrame(avg_temp_c) OVER (
                PARTITION BY city ORDER BY week_start_date
                ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
            ),
            2
        ) AS wow_temp_delta_c,

        -- week-over-week precipitation change
        round(
            total_precipitation_mm
            - lagInFrame(total_precipitation_mm) OVER (
                PARTITION BY city ORDER BY week_start_date
                ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
            ),
            3
        ) AS wow_precipitation_delta_mm,

        -- 4-week rolling average temperature
        round(
            avg(avg_temp_c) OVER (
                PARTITION BY city ORDER BY week_start_date
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS rolling_4w_avg_temp_c

    FROM weekly_agg

)

SELECT *
FROM with_deltas
ORDER BY city, week_start_date
