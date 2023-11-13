import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")

with engine.connect() as conn:
    conn.execute(text("""--sql
        create table if not exists weather (
            time timestamptz not null,
            latitude float4,
            longitude float4,
            temperature_2m float4,
            zonal_wind_10m float4,
            meridional_wind_10m float4,
            total_cloud_cover float4,
            total_precipitation float4,
            snowfall float4
        );
    """))

    conn.execute(text("select create_hypertable('weather', 'time');"))

    conn.commit()
