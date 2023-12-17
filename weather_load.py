import os
import time
import logging
import xarray as xr

from dotenv import load_dotenv
from tqdm import tqdm
from joblib import Parallel, delayed
from sqlalchemy import create_engine, text


logging.basicConfig(format='%(asctime)s %(message)s')

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
CSV_PATH = os.getenv("CSV_PATH")

def load_data(n):
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")

    with engine.connect() as conn:
        t1 = time.perf_counter()

        conn.execute(text(f"""--sql
            copy weather
            from '{CSV_PATH}/weather_hour{n}.csv'
            delimiter ','
            csv header;
        """))
        conn.commit()
    
        t2 = time.perf_counter()
        print(f"[n={n:03d}] inserted 1,038,240 rows in {t2 - t1:.3f} seconds = {int(1038240 / (t2 - t1)):,} inserts/second")

    return

t1 = time.perf_counter()

Parallel(n_jobs=8)(
    delayed(load_data)(n)
    for n in range(100)
)

t2 = time.perf_counter()
print(f"Inserted {100 * 1038240:,} rows in {t2 - t1:.3f} seconds = {int(100*1038240 / (t2 - t1)):,} inserts/second")