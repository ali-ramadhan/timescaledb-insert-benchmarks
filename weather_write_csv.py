import os
import time
import logging
import xarray as xr

from dotenv import load_dotenv
from tqdm import tqdm
from joblib import Parallel, delayed
from sqlalchemy import create_engine, text


class Timer:
    def __init__(self, message="Execution time"):
        self.message = message

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start
        print(f"{self.message}: {self.interval:.4f} seconds.")


logging.basicConfig(format='%(asctime)s %(message)s')

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RAMDISK_PATH = os.getenv("RAMDISK_PATH")

nc_filepaths = [
    "e5.oper.an.sfc.128_164_tcc.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_165_10u.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_166_10v.ll025sc.1995030100_1995033123.nc",
    "e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc",
    "e5_snowfall_199305.nc",
    "e5_total_precipitation_199305.nc"
]

cols_renamed = {
    "VAR_2T": "temperature_2m",
    "VAR_10U": "zonal_wind_10m",
    "VAR_10V": "meridional_wind_10m",
    "TCC": "total_cloud_cover",
    "tp": "total_precipitation",
    "sf": "snowfall"
}

def write_csv(n):
    with Timer(f"[n={n:03d}] Loading data"):
        ds = xr.open_mfdataset(nc_filepaths)
        df = ds.isel(time=0).to_dataframe().reset_index()

        df.drop(columns=["utc_date"], inplace=True)
        df.rename(columns=cols_renamed, inplace=True)

        df["temperature_2m"] -= 273.15  # Kelvin to Celsius
        df["total_precipitation"] *= 1000  # m to mm
        df["snowfall"] *= 1000  # m to mm

        df.rename(columns={"longitude": "longitude_east"}, inplace=True)
        df["longitude"] = df["longitude_east"].apply(lambda x: x - 360 if x > 180 else x)
        df.drop(columns=["longitude_east"], inplace=True)

        

        df = df[[
            "time",
            "latitude",
            "longitude",
            "temperature_2m",
            "zonal_wind_10m",
            "meridional_wind_10m",
            "total_cloud_cover",
            "total_precipitation",
            "snowfall"
        ]]

    with Timer(f"[n={n:03d}] Saving csv"):
        df.to_csv(
            f"{RAMDISK_PATH}/weather_hour{n}.csv",
            index=False,
            header=False,
            date_format="%Y-%m-%d %H:%M:%S"
        )

    return

Parallel(n_jobs=24)(
    delayed(write_csv)(n)
    for n in tqdm(range(100))
)
