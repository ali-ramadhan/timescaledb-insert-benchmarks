import os
import argparse
from pathlib import Path

import dotenv
from sqlalchemy import text

from write_csv import weather_dataframe, write_csv
from timer import Timer
from utils import get_sqlalchemy_engine, get_psycopg3_connection, count_lines

dotenv.load_dotenv()

CSV_PATH = os.getenv("CSV_PATH")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using COPY statements."
    )

    parser.add_argument(
        "--hours",
        type=int,
        help="How many hours of ERA5 data to load. Each hour is roughly 1 million rows.",
        required=True
    )

    parser.add_argument(
        "--method",
        choices=["copy_csv", "psycopg3"],
        help="How to copy data into the table.",
        required=True
    )

    parser.add_argument(
        "--benchmarks-file",
        type=str,
        help="Filepath to output benchmarks to a CSV file.",
        required=True
    )
    
    return parser.parse_args()

def log_benchmark(args, hour, num_rows, full_timer, copy_timer):
    filepath = args.benchmarks_file
    
    # Create file and write CSV header
    if not Path(filepath).exists():
        with open(filepath, "a") as file:
            file.write(
                "method,hour,num_rows,seconds_full,rate_full,units_full,"
                "seconds_copy,rate_copy,units_copy\n"
            )
    
    with open(filepath, "a") as file:
        file.write(
            f"{args.method},{hour},{num_rows},{full_timer.interval},"
            f"{full_timer.rate},{full_timer.units},{copy_timer.interval},"
            f"{copy_timer.rate},{copy_timer.units}\n"
        )
    
    return

def copy_data_using_psycopg3(n, args):
    df = weather_dataframe(n)
    
    full_timer = Timer(
        f"COPYing data using psycopg3 cursor (counting overhead)",
        n=df.shape[0],
        units="inserts"
    )

    copy_timer = Timer(
        f"COPYing data using psycopg3 cursor",
        n=df.shape[0],
        units="inserts"
    )

    with get_psycopg3_connection() as conn, conn.cursor() as cur, full_timer:
        with Timer("Constructing data tuples"):
            data_tuples = []
            for row in df.itertuples(index=False):
                data_tuples.append(tuple(row))
        
        with cur.copy("""
            copy weather (
                time,
                location_id,
                latitude,
                longitude,
                temperature_2m,
                zonal_wind_10m,
                meridional_wind_10m,
                total_cloud_cover,
                total_precipitation,
                snowfall
            ) from stdin
            """
        ) as copy, copy_timer:
            for row in data_tuples:
                copy.write_row(row)
    
        conn.commit()
    
    log_benchmark(args, n, df.shape[0], full_timer, copy_timer)

    return

def copy_data_using_csv(n, args):
    df = weather_dataframe(n)

    full_timer = Timer(
        f"COPYing data using COPY (counting overhead)",
        n=df.shape[0],
        units="inserts"
    )

    copy_timer = Timer(
        f"COPYing data using COPY",
        n=df.shape[0],
        units="inserts"
    )

    with full_timer:
        csv_filepath = f"{CSV_PATH}/weather_hour{n}.csv"
        write_csv(df, csv_filepath)

        engine = get_sqlalchemy_engine()

        with engine.connect() as conn, copy_timer:
            conn.execute(text(f"""--sql
                copy weather
                from '{csv_filepath}'
                delimiter ','
                csv header;
            """))
            conn.commit()

    log_benchmark(args, n, df.shape[0], full_timer, copy_timer)

    return

def main(args):
    with Timer(f"COPYing {args.hours} hours of data using {args.method}"):
        if args.method == "psycopg3":
            for n in range(args.hours):
                copy_data_using_psycopg3(n, args)
        elif args.method == "copy_csv":
            for n in range(args.hours):
                copy_data_using_csv(n, args)
    return

if __name__ == "__main__":
    main(parse_args())
