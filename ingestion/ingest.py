"""
Weather Data Ingestion Pipeline
Source: Open-Meteo API (https://open-meteo.com/) - No API key required
Targets: PostgreSQL (OLTP) → ClickHouse (OLAP)
"""

import os
import time
import requests
import pandas as pd
import psycopg2
import clickhouse_connect
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

CITIES = [
    {"name": "Kigali",    "country": "Rwanda",   "lat": -1.9441, "lon": 30.0619},
    {"name": "Nairobi",   "country": "Kenya",    "lat": -1.2921, "lon": 36.8219},
    {"name": "Kampala",   "country": "Uganda",   "lat":  0.3476, "lon": 32.5825},
    {"name": "Dar es Salaam", "country": "Tanzania", "lat": -6.7924, "lon": 39.2083},
    {"name": "Lagos",     "country": "Nigeria",  "lat":  6.5244, "lon": 3.3792},
]

# Fetch last 30 days of hourly data
END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

API_URL = "https://api.open-meteo.com/v1/forecast"

PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbname":   os.getenv("POSTGRES_DB"),
}

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", 8123))
CH_DB       = os.getenv("CLICKHOUSE_DB", "weather_analytics")
CH_USER     = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


# ─── STEP 1: FETCH FROM API ───────────────────────────────────────────────────

def fetch_weather(city: dict) -> pd.DataFrame:
    """Pull hourly weather data from Open-Meteo for one city."""
    params = {
        "latitude":   city["lat"],
        "longitude":  city["lon"],
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weathercode",
        ],
        "start_date": START_DATE,
        "end_date":   END_DATE,
        "timezone":   "Africa/Nairobi",
    }

    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    df = pd.DataFrame({
        "city":              city["name"],
        "country":           city["country"],
        "latitude":          city["lat"],
        "longitude":         city["lon"],
        "recorded_at":       pd.to_datetime(hourly["time"]),
        "temperature_c":     hourly["temperature_2m"],
        "humidity_pct":      hourly["relative_humidity_2m"],
        "precipitation_mm":  hourly["precipitation"],
        "wind_speed_kmh":    hourly["wind_speed_10m"],
        "weather_code":      hourly["weathercode"],
        "ingested_at":       datetime.now(timezone.utc).replace(tzinfo=None),
    })
    return df


# ─── STEP 2: LOAD INTO POSTGRESQL ─────────────────────────────────────────────

def setup_postgres(conn):
    """Create raw weather table in PostgreSQL if not exists."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather (
                id               SERIAL PRIMARY KEY,
                city             VARCHAR(100)   NOT NULL,
                country          VARCHAR(100)   NOT NULL,
                latitude         NUMERIC(9, 6)  NOT NULL,
                longitude        NUMERIC(9, 6)  NOT NULL,
                recorded_at      TIMESTAMP      NOT NULL,
                temperature_c    NUMERIC(5, 2),
                humidity_pct     NUMERIC(5, 2),
                precipitation_mm NUMERIC(7, 3),
                wind_speed_kmh   NUMERIC(7, 3),
                weather_code     INTEGER,
                ingested_at      TIMESTAMP      DEFAULT NOW(),
                UNIQUE (city, recorded_at)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_raw_weather_city_time
                ON raw_weather (city, recorded_at);
        """)
        conn.commit()
    print("✅ PostgreSQL table ready.")


def load_to_postgres(conn, df: pd.DataFrame):
    """Insert rows into PostgreSQL, skipping duplicates."""
    records = df[[
        "city", "country", "latitude", "longitude", "recorded_at",
        "temperature_c", "humidity_pct", "precipitation_mm",
        "wind_speed_kmh", "weather_code", "ingested_at",
    ]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO raw_weather (
                city, country, latitude, longitude, recorded_at,
                temperature_c, humidity_pct, precipitation_mm,
                wind_speed_kmh, weather_code, ingested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, recorded_at) DO NOTHING;
        """, records)
        conn.commit()
    print(f"  ↳ {len(records)} rows upserted into PostgreSQL.")


# ─── STEP 3: REPLICATE INTO CLICKHOUSE ────────────────────────────────────────

def setup_clickhouse(client):
    """Create ClickHouse database and raw table."""
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.raw_weather (
            city             String,
            country          String,
            latitude         Float64,
            longitude        Float64,
            recorded_at      DateTime,
            temperature_c    Nullable(Float32),
            humidity_pct     Nullable(Float32),
            precipitation_mm Nullable(Float32),
            wind_speed_kmh   Nullable(Float32),
            weather_code     Nullable(Int32),
            ingested_at      DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(recorded_at)
        ORDER BY (city, recorded_at)
        SETTINGS index_granularity = 8192;
    """)
    print("✅ ClickHouse table ready.")


def load_to_clickhouse(client, df: pd.DataFrame):
    """Insert rows from Postgres-bound df into ClickHouse."""
    ch_df = df[[
        "city", "country", "latitude", "longitude", "recorded_at",
        "temperature_c", "humidity_pct", "precipitation_mm",
        "wind_speed_kmh", "weather_code", "ingested_at",
    ]].copy()

    # ClickHouse needs Python datetime, not pd.Timestamp with tz
    ch_df["recorded_at"] = ch_df["recorded_at"].dt.tz_localize(None)
    ch_df["ingested_at"] = ch_df["ingested_at"].dt.tz_localize(None) \
        if hasattr(ch_df["ingested_at"].dt, "tz_localize") else ch_df["ingested_at"]

    client.insert_df(f"{CH_DB}.raw_weather", ch_df)
    print(f"  ↳ {len(ch_df)} rows inserted into ClickHouse.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Weather Pipeline — Ingestion Layer")
    print(f"  Period: {START_DATE} → {END_DATE}")
    print("=" * 60)

    # Connect
    pg_conn  = psycopg2.connect(**PG_CONFIG)
    ch_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
    )

    # Setup schemas
    setup_postgres(pg_conn)
    setup_clickhouse(ch_client)

    # Ingest per city
    for city in CITIES:
        print(f"\n📍 Fetching: {city['name']}, {city['country']}")
        try:
            df = fetch_weather(city)
            load_to_postgres(pg_conn, df)
            load_to_clickhouse(ch_client, df)
        except Exception as e:
            print(f"  ⚠️  Failed for {city['name']}: {e}")
        time.sleep(1)  # be polite to the API

    pg_conn.close()
    ch_client.close()

    print("\n✅ Ingestion complete.")


if __name__ == "__main__":
    main()