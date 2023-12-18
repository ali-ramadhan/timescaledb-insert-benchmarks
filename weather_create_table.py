from sqlalchemy import text
from utils import get_engine

if __name__ == "__main__":
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("""--sql
            create table if not exists weather (
                time timestamptz not null,
                location_id int,
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
