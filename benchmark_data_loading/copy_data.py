import argparse
from pathlib import Path

from sqlalchemy import text

from write_csv import weather_dataframe
from timer import Timer
from utils import get_sqlalchemy_engine, get_psycopg3_connection

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using row-by-row inserts."
    )

    parser.add_argument(
        "--csv-filepath",
        type=str,
        help="Path to CSV file containing data produced by write_csv.py.",
        required=True
    )

    parser.add_argument(
        "--method",
        choices=["pandas", "psycopg3", "sqlalchemy"],
        help="How to copy data into the table.",
        required=True
    )

    # parser.add_argument(
    #     "--benchmarks-file",
    #     type=str,
    #     help="Filepath to output benchmarks to a CSV file.",
    #     required=True
    # )
    
    return parser.parse_args()

def copy_data_using_sqlalchemy(timer, args):
    engine = get_sqlalchemy_engine()
    with engine.connect() as conn, timer:
        conn.execute(text(f"""--sql
            copy weather
            from '{args.csv_filepath}'
            delimiter ','
            csv header;
        """))
        conn.commit()
    return

def main(args):
    timer = Timer(
        f"COPYing data using {args.method}",
        n=1038240,
        units="inserts"
    )

    if args.method == "psycopg3":
        copy_data_using_psycopg3(timer, args)
    elif args.method == "sqlalchemy":
        copy_data_using_sqlalchemy(timer, args)
    elif args.method == "pandas":
        copy_data_using_pandas(timer, args)

    # log_benchmark(args, timer)

    return

if __name__ == "__main__":
    main(parse_args())