import os
import argparse
from pathlib import Path

import dotenv
from sqlalchemy import text
from joblib import Parallel, delayed

from write_csv import weather_dataframe, write_csv
from timer import Timer
from utils import get_sqlalchemy_engine, get_psycopg3_connection, num_rows

dotenv.load_dotenv()

CSV_PATH = os.getenv("CSV_PATH")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using COPY statements."
    )

    parser.add_argument(
        "--method",
        choices=["copy_csv", "psycopg3"],
        help="How to copy data into the table.",
        required=True
    )

    parser.add_argument(
        "--table-type",
        choices=["regular", "hyper"],
        help="Create a regular PostgreSQL table or a TimescaleDB hypertable.",
        required=True
    )

    parser.add_argument(
        "--workers",
        type=int,
        help="Number of parallel workers.",
        required=True
    )

    parser.add_argument(
        "--hours",
        type=int,
        help="How many hours of ERA5 data to load. Each hour is roughly 1 million rows.",
        required=True
    )

    parser.add_argument(
        "--benchmarks-file",
        type=str,
        help="Filepath to output benchmarks to a CSV file.",
        required=True
    )

    parser.add_argument(
        "--parallel-benchmarks-file",
        type=str,
        help="Filepath to output parallel benchmarks to a CSV file."
    )

    return parser.parse_args()

def log_benchmark(args, hour, num_rows, full_timer, copy_timer):
    filepath = args.benchmarks_file
    
    # Create file and write CSV header
    if not Path(filepath).exists():
        with open(filepath, "a") as file:
            file.write(
                "method,table_type,workers,hour,num_rows,"
                "seconds_full,rate_full,units_full,"
                "seconds_copy,rate_copy,units_copy\n"
            )
    
    with open(filepath, "a") as file:
        file.write(
            f"{args.method},{args.table_type},{args.workers},{hour},{num_rows},"
            f"{full_timer.interval},{full_timer.rate},{full_timer.units},"
            f"{copy_timer.interval},{copy_timer.rate},{copy_timer.units}\n"
        )
    
    return

def log_parallel_benchmark(args, timer):
    filepath = args.parallel_benchmarks_file
    
    # Create file and write CSV header
    if not Path(filepath).exists():
        with open(filepath, "a") as file:
            file.write(
                "method,table_type,workers,hours,num_rows,"
                "seconds,rate,units\n"
            )
    
    with open(filepath, "a") as file:
        file.write(
            f"{args.method},{args.table_type},{args.workers},{args.hours},{num_rows(args.hours)},"
            f"{timer.interval},{timer.rate},{timer.units}\n"
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
    if args.method == "psycopg3":
        copy_func = copy_data_using_psycopg3
    elif args.method == "copy_csv":
        copy_func = copy_data_using_csv
    
    timer = Timer(
        f"COPYing {args.hours} hours of data using {args.method} "
        f"with {args.workers} workers",
        n=num_rows(args.hours),
        units="inserts"
    )

    with timer:
        if args.workers == 1:
            for n in range(args.hours):
                copy_func(n, args)
        else:
            Parallel(n_jobs=args.workers)(
                delayed(copy_func)(n, args)
                for n in range(args.hours)
            )
    
    if args.parallel_benchmarks_file:
        log_parallel_benchmark(args, timer)      

    return

if __name__ == "__main__":
    main(parse_args())
