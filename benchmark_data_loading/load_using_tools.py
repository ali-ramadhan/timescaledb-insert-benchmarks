import os
import argparse
from pathlib import Path

import dotenv

from write_csv import weather_dataframe, write_csv
from timer import Timer
from utils import sqlalchemy_connection_string, run_in_container, num_rows

dotenv.load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
CSV_PATH = os.getenv("CSV_PATH")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using external tools."
    )

    parser.add_argument(
        "--hours",
        type=int,
        help="How many hours of ERA5 data to load. Each hour is roughly 1 million rows.",
        required=True
    )

    parser.add_argument(
        "--method",
        choices=["pg_bulkload", "timescaledb_parallel_copy"],
        help="How to copy data into the table.",
        required=True
    )

    parser.add_argument(
        "--benchmarks-file",
        type=str,
        help="Filepath to output benchmarks to a CSV file.",
        required=True
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of parallel workers.",
        required=True
    )

    return parser.parse_args()

def log_benchmark(args, timer, hour):
    filepath = args.benchmarks_file
    
    # Create file and write CSV header
    if not Path(filepath).exists():
        with open(filepath, "a") as file:
            file.write(
                "method,workers,hour,num_rows,"
                "seconds,rate,units\n"
            )
    
    with open(filepath, "a") as file:
        file.write(
            f"{args.method},{args.workers},{hour},{num_rows(1)},"
            f"{timer.interval},{timer.rate},{timer.units}\n"
        )
    
    return

def load_data_using_pg_bulkload(args, timer):
    run_in_container([
        "psql",
        "-U", POSTGRES_USER,
        "-d", POSTGRES_DB_NAME,
        "-c", "CREATE EXTENSION if not exists pg_bulkload;"
    ])

    for n in range(args.hours):
        timer = Timer(
            f"COPYing hour {n} using {args.method} with {args.workers} workers",
            n=num_rows(1),
            units="inserts"
        )

        cmd = [
            "pg_bulkload",
            f"--host={POSTGRES_HOST}",
            f"--username={POSTGRES_USER}",
            f"--dbname={POSTGRES_DB_NAME}",
            f"--input", f"{CSV_PATH}/weather_hour{n}.csv",
            "--output", "weather",
            "-o", "writer=parallel"
        ]

        with timer:
            run_in_container(cmd)
        
        log_benchmark(args, timer, n)

    return

def load_data_using_tpc(args, timer):
    for n in range(args.hours):
        timer = Timer(
            f"COPYing hour {n} using {args.method} with {args.workers} workers",
            n=num_rows(1),
            units="inserts"
        )

        cmd = [
            "timescaledb-parallel-copy",
            "--verbose",
            "--reporting-period", "10s",
            "--connection", sqlalchemy_connection_string(),
            "--table", "weather",
            "--batch-size", "10000",
            "--workers", f"{args.workers}",
            "--file", f"{CSV_PATH}/weather_hour{n}.csv"
        ]

        with timer:
            run_in_container(cmd)
        
        log_benchmark(args, timer, n)
    
    return

def main(args):
    timer = Timer(
        f"COPYing {args.hours} hours of data using {args.method} "
        f"with {args.workers} workers",
        n=num_rows(args.hours),
        units="inserts"
    )

    if args.method == "pg_bulkload":
        load_data_using_pg_bulkload(args, timer)
    elif args.method == "timescaledb_parallel_copy":
        load_data_using_tpc(args, timer)

    return

if __name__ == "__main__":
    main(parse_args())
