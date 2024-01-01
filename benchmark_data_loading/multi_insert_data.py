import argparse
from pathlib import Path

from sqlalchemy import text

from write_csv import weather_dataframe
from timer import Timer
from utils import get_sqlalchemy_engine, get_psycopg3_connection

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data into the weather table using multi-valued inserts."
    )

    parser.add_argument(
        "--method",
        choices=["pandas", "psycopg3", "sqlalchemy"],
        help="How to insert rows into the table.",
        required=True
    )

    parser.add_argument(
        "--num-rows",
        type=int,
        help="Number of rows to insert.",
        required=True
    )

    parser.add_argument(
        "--table-type",
        choices=["regular", "hyper"],
        help="Create a regular PostgreSQL table or a TimescaleDB hypertable.",
        required=True
    )

    parser.add_argument(
        "--benchmarks-file",
        type=str,
        help="Filepath to output benchmarks to a CSV file.",
        required=True
    )
    
    return parser.parse_args()

def log_benchmark(args, timer):
    filepath = args.benchmarks_file
    
    # Create file and write CSV header
    if not Path(filepath).exists():
        with open(filepath, "a") as file:
            file.write("method,table_type,num_rows,seconds,rate,units\n")
    
    with open(filepath, "a") as file:
        file.write(
            f"{args.method},{args.table_type},{args.num_rows},"
            f"{timer.interval},{timer.rate},{timer.units}\n"
        )
    
    return

def batch_insert_data_using_psycopg3(df, timer, args):
    with get_psycopg3_connection() as conn, conn.cursor() as cur, timer:
        insert_query = """
            insert into weather (
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
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with Timer("Constructing data tuples"):
            data_tuples = []
            for row in df.itertuples(index=False):
                data_tuples.append(tuple(row))

        cur.executemany(insert_query, data_tuples)
        conn.commit()

    return

def batch_insert_data_using_sqlalchemy(df, timer, args):
    engine = get_sqlalchemy_engine()

    with engine.connect() as conn, timer:
        insert_query = """
            insert into weather (
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
            ) values (
                :time,
                :location_id,
                :latitude,
                :longitude,
                :temperature_2m,
                :zonal_wind_10m,
                :meridional_wind_10m,
                :total_cloud_cover,
                :total_precipitation,
                :snowfall
            )
        """
        with Timer("Constructing data dicts"):
            data_dicts = df.to_dict("records")
        
        conn.execute(text(insert_query), data_dicts)
    
    return

def batch_insert_data_using_pandas(df, timer, args):
    engine = get_sqlalchemy_engine()

    with timer:
        df.to_sql("weather", engine, if_exists="append", index=False)
    
    return

def main(args):
    df = weather_dataframe(0)
    df = df.head(args.num_rows)

    timer = Timer(
        f"Batch inserting data using {args.method}",
        n=len(df.index),
        units="inserts"
    )

    if args.method == "psycopg3":
        batch_insert_data_using_psycopg3(df, timer, args)
    elif args.method == "sqlalchemy":
        batch_insert_data_using_sqlalchemy(df, timer, args)
    elif args.method == "pandas":
        batch_insert_data_using_pandas(df, timer, args)

    log_benchmark(args, timer)

    return

if __name__ == "__main__":
    main(parse_args())