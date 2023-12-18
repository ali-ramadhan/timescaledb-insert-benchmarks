import argparse
from sqlalchemy import text
from utils import get_engine

def parse_args():
    parser = argparse.ArgumentParser(description="Create a weather table in Postgres.")

    parser.add_argument(
        "--unlogged",
        dest="unlogged",
        action="store_true",
        default=False,
        help="Make the table unlogged."
    )

    parser.add_argument(
        "--create-hypertable",
        dest="create_hypertable",
        action="store_true",
        default=False,
        help="Convert the table into a TimescaleDB hypertable."
    )
    
    return parser.parse_args()


def main(args):
    engine = get_engine()

    with engine.connect() as conn:
        unlogged = "unlogged" if args.unlogged else ""
        table_creation_query = f"""--sql
            create {unlogged} table if not exists weather (
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
        """

        conn.execute(text(table_creation_query))

        if args.create_hypertable:
            conn.execute(text("select create_hypertable('weather', 'time');"))

        conn.commit()

if __name__ == "__main__":
    main(parse_args())
