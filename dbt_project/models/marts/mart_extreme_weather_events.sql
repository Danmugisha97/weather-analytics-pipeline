-- mart_extreme_weather_events.sql
-- Flags days that breach weather-severity thresholds.
-- Demonstrates operational use: alerting, risk dashboards, anomaly baselines.

WITH daily AS (

    SELECT * FROM {{ ref('mart_daily_weather_summary') }}

),

flagged AS (

    SELECT
        city,
        country,
        record_date,
        avg_temp_c,
        max_temp_c,
        min_temp_c,
        total_precipitation_mm,
        avg_wind_speed_kmh,
        max_wind_speed_kmh,
        avg_humidity_pct,
        dominant_weather,
        heat_index_approx,

        -- ── Severity flags ────────────────────────────────────────────────
        -- Heatwave: max temp above 35 °C
        (max_temp_c > 35)                        AS is_heatwave,
        -- Cold snap: min temp below 10 °C (relevant for equatorial cities)
        (min_temp_c < 10)                        AS is_cold_snap,
        -- Heavy rain: daily total > 20 mm
        (total_precipitation_mm > 20)            AS is_heavy_rain,
        -- Extreme rain: daily total > 50 mm (flash-flood risk)
        (total_precipitation_mm > 50)            AS is_extreme_rain,
        -- High wind: average wind speed > 40 km/h
        (avg_wind_speed_kmh > 40)                AS is_high_wind,
        -- Storm day: dominant condition contains thunder
        (dominant_weather = 'Thunderstorm')      AS is_storm_day,
        -- High humidity: average humidity above 85 %
        (avg_humidity_pct > 85)                  AS is_high_humidity,

        -- ── Composite severity score (0–7) ────────────────────────────────
        toUInt8(max_temp_c > 35)
        + toUInt8(min_temp_c < 10)
        + toUInt8(total_precipitation_mm > 20)
        + toUInt8(total_precipitation_mm > 50)
        + toUInt8(avg_wind_speed_kmh > 40)
        + toUInt8(dominant_weather = 'Thunderstorm')
        + toUInt8(avg_humidity_pct > 85)         AS severity_score

    FROM daily

)

SELECT
    *,
    CASE
        WHEN severity_score = 0 THEN 'Normal'
        WHEN severity_score = 1 THEN 'Advisory'
        WHEN severity_score = 2 THEN 'Watch'
        WHEN severity_score >= 3 THEN 'Warning'
    END AS severity_level

FROM flagged
WHERE severity_score > 0          -- only days with at least one flag
ORDER BY city, record_date
