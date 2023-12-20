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


# with psycopg.connect("user=postgres password=oceans host=localhost port=5432 dbname=climarisk") as conn:
#     with conn.cursor() as cur:

#         ds = xr.open_dataset("/storage3/alir/era5/2t/e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc")
#         ds.VAR_2T.load()

#         for n, t in enumerate(tqdm(ds.time.values, desc="time", position=0)):
#             with cur.copy("COPY weather (latitude, longitude, time, temperature) FROM STDIN") as copy:
#                 for i, lat in enumerate(tqdm(ds.latitude.values, desc="latitude", position=1, leave=False)):
#                     for j, lon in enumerate(ds.longitude.values):
#                         temp = ds.VAR_2T.values[n, i, j] - 273.15
#                         record = (lat, lon, pd.Timestamp(t), temp)
#                         copy.write_row(record)

#             # for i, lat in enumerate(tqdm(ds.latitude.values, desc="latitude", position=1, leave=False)):
#             #     for j, lon in enumerate(ds.longitude.values):
#             #         temp = ds.VAR_2T.values[n, i, j] - 273.15
#             #         cur.execute(f"INSERT INTO weather (latitude, longitude, time, temperature) VALUES ({lat}, {lon}, '{t}', {temp})")

#             conn.commit()