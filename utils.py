import os
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def get_engine():
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")
    return engine

class Timer:
    def __init__(self, message="Execution time", n=None, units="operations"):
        self.message = message
        self.n = n
        self.units = units

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
        else:
            print(f"{self.message}: {self.interval:.4f} seconds.")