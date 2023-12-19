import argparse

from tqdm import tqdm
from sqlalchemy import text

from weather_write_csv import weather_dataframe
from utils import Timer, get_engine

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using inserts."
    )

    parser.add_argument(
        "--num-rows",
        type=int,
        help="Number of rows to insert."
    )

    parser.add_argument(
        "--method",
        choices=["sqlalchemy", "pandas"],
        help="How to insert rows into the table."
    )

    parser.add_argument(
        "--benchmark",
        dest="benchmark",
        action="store_true",
        default=False,
        help="Benchmark insertion performance to a CSV file."
    )
    
    return parser.parse_args()

def main(args):
    df = weather_dataframe(0)
    df = df.head(args.num_rows)
    with Timer(
        f"Inserting data row-by-row using {args.method}",
        n=len(df.index),
        units="inserts",
        filepath=f"benchmark_insert_{args.method}_{args.num_rows}rows.csv"
    ):
        if args.method == "pandas":
            engine = get_engine()
            df.to_sql("weather", engine, if_exists="append", index=False)
        elif args.method == "sqlalchemy":
            engine = get_engine()
            with engine.connect() as conn:
                for index, row in df.iterrows():
                    insert_query = """
                        INSERT INTO weather (time, location_id, latitude, longitude, temperature_2m, zonal_wind_10m, meridional_wind_10m, total_cloud_cover, total_precipitation, snowfall) 
                        VALUES (:time, :location_id, :latitude, :longitude, :temperature_2m, :zonal_wind_10m, :meridional_wind_10m, :total_cloud_cover, :total_precipitation, :snowfall)
                    """

                    conn.execute(text(insert_query), {
                        "time": row["time"],
                        "location_id": row["location_id"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                        "temperature_2m": row["temperature_2m"],
                        "zonal_wind_10m": row["zonal_wind_10m"],
                        "meridional_wind_10m": row["meridional_wind_10m"],
                        "total_cloud_cover": row["total_cloud_cover"],
                        "total_precipitation": row["total_precipitation"],
                        "snowfall": row["snowfall"]
                    })

    return

if __name__ == "__main__":
    main(parse_args())
