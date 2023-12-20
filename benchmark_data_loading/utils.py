import os
import time
from pathlib import Path

import dotenv
import sqlalchemy
import psycopg

dotenv.load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def get_sqlalchemy_engine():
    engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}"
    )
    return engine

def get_psycopg3_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"dbname={POSTGRES_DB_NAME} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )
    return psycopg.connect(connection_string)

class Timer:
    def __init__(
        self,
        message="Execution time",
        n=None,
        units="operations",
        filepath=None
    ):
        self.message = message
        self.n = n
        self.units = units
        self.filepath = filepath

        # Write CSV header
        if self.filepath and not Path(self.filepath).exists():
            with open(self.filepath, "a") as file:
                if self.n and self.units:
                    file.write("seconds,n,rate,units\n")
                else:
                    file.write("seconds")

    def __enter__(self):
        print(f"{self.message}: started.")
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

        if self.n and self.units:
            rate = self.n / self.interval
            print(f"{self.message}: {self.interval:.4f} seconds (n={self.n}, {rate:.2f} {self.units} per second).")
            if self.filepath:
                with open(self.filepath, "a") as file:
                    file.write(f"{self.interval},{self.n},{rate},{self.units}\n")
        else:
            print(f"{self.message}: {self.interval:.4f} seconds.")
            if self.filepath:
                with open(self.filepath, "a") as file:
                    file.write(f"{self.interval}\n")
