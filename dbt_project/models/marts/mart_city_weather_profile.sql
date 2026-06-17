-- mart_city_weather_profile.sql
-- All-time climate profile per city derived from the daily summary mart.
-- Useful for benchmarking: "how does today compare to this city's historical baseline?"

WITH daily AS (

    SELECT * FROM {{ ref('mart_daily_weather_summary') }}

)

SELECT
    city,
    country,
    -- record window
    min(record_date)                             AS first_record_date,
    max(record_date)                             AS last_record_date,
    count(*)                                     AS total_days_observed,
    -- temperature baseline
    round(avg(avg_temp_c),    2)                 AS overall_avg_temp_c,
    round(min(min_temp_c),    2)                 AS all_time_min_temp_c,
    round(max(max_temp_c),    2)                 AS all_time_max_temp_c,
    round(max(max_temp_c) - min(min_temp_c), 2)  AS temp_range_c,

    -- humidity baseline
    round(avg(avg_humidity_pct), 2)              AS overall_avg_humidity_pct,

    -- precipitation
    round(avg(total_precipitation_mm), 3)        AS avg_daily_precipitation_mm,
    round(max(total_precipitation_mm), 3)        AS wettest_day_precipitation_mm,
    countIf(total_precipitation_mm > 0)          AS rainy_days_count,
    round(
        countIf(total_precipitation_mm > 0) * 100.0 / count(*), 1
    )                                            AS rainy_days_pct,

    -- wind
    round(avg(avg_wind_speed_kmh), 2)            AS overall_avg_wind_speed_kmh,
    round(max(max_wind_speed_kmh), 2)            AS all_time_max_wind_speed_kmh,

    -- most common weather condition across the full window
    topK(1)(dominant_weather)[1]                 AS most_frequent_weather,

    -- heat index baseline
    round(avg(heat_index_approx), 2)             AS avg_heat_index

FROM daily
GROUP BY city, country
ORDER BY city
