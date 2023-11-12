import os
import xarray as xr

from dotenv import load_dotenv
from tqdm import tqdm
from joblib import Parallel, delayed
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def insert_time_sliced_date(n):
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")

    ds = xr.open_dataset("e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc")
    df = ds.isel(time=n).to_dataframe().reset_index()

    df.rename(columns={"VAR_2T": "temperature"}, inplace=True)
    df.drop("utc_date", axis=1, inplace=True)
    df["temperature"] -= 273.15

    df.to_sql("weather", engine, if_exists="append", index=False, chunksize=1000000)

    return

ds = xr.open_dataset("e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc")

Parallel(n_jobs=8)(
    delayed(insert_time_sliced_date)(n)
    for n in tqdm(range(len(ds.time)))
)
