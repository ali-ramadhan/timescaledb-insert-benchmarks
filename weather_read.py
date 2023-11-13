import os
import xarray as xr

from dotenv import load_dotenv
from tqdm import tqdm
from joblib import Parallel, delayed
from sqlalchemy import create_engine

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

nc_filepaths = [
    "e5.oper.an.sfc.128_164_tcc.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_165_10u.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_166_10v.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc",
    "snowfall_199305.nc",
    "total_precipitation_199305.nc"
]

cols_renamed = {
    "VAR_2T": "temperature_2m",
    "VAR_10U": "zonal_wind_10m",
    "VAR_10V": "meridional_wind_10m",
    "TCC": "total_cloud_cover",
    "tp": "total_precipitation",
    "sf": "snowfall"
}

def insert_time_sliced_data(n):
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")

    ds = xr.open_mfdataset(nc_filepaths)
    df = ds.isel(time=n).to_dataframe().reset_index()

    df.drop("utc_date", axis=1, inplace=True)
    df.rename(columns=cols_renamed, inplace=True)

    df["temperature_2m"] -= 273.15  # Kelvin to Celsius
    df["total_precipitation"] *= 1000  # m to mm
    df["snowfall"] *= 1000  # m to mm

    df.to_sql("weather", engine, if_exists="append", index=False)

    return

ds = xr.open_mfdataset(nc_filepaths)

Parallel(n_jobs=6)(
    delayed(insert_time_sliced_data)(n)
    for n in tqdm(range(len(ds.time)))
)
