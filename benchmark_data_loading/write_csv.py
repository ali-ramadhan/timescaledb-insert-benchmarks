import logging
import xarray as xr

from tqdm import tqdm
from joblib import Parallel, delayed

from timer import Timer

logging.basicConfig(format='%(asctime)s %(message)s')

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

def latlon_to_location_id(lats, lons, dlat=0.25, dlon=0.25, min_lat=-90, min_lon=-179.75):
    n_lats = int(180 / dlat) + 1  # +1 to include both endpoints (poles)
    n_lons = int(360 / dlon)
    lat_indices = ((lats - min_lat) / dlat).astype(int)
    lon_indices = ((lons - min_lon) / dlon).astype(int)
    return lat_indices * n_lons + lon_indices + 1

def weather_dataframe(n):
    with Timer(f"Loading data"):
        ds = xr.open_mfdataset(nc_filepaths)
        df = ds.isel(time=n).to_dataframe().reset_index()

        df.drop(columns=["utc_date"], inplace=True)
        df.rename(columns=cols_renamed, inplace=True)

        df["temperature_2m"] -= 273.15  # Kelvin to Celsius
        df["total_precipitation"] *= 1000  # m to mm
        df["snowfall"] *= 1000  # m to mm

        # Convert latitude from [0, 360) degrees East to [-180, 180) degrees East.
        df.rename(columns={"longitude": "longitude_east"}, inplace=True)
        df["longitude"] = df["longitude_east"].apply(lambda x: x - 360 if x > 180 else x)
        df.drop(columns=["longitude_east"], inplace=True)

        df["location_id"] = latlon_to_location_id(df.latitude, df.longitude)

        df = df[[
            "time",
            "location_id",
            "latitude",
            "longitude",
            "temperature_2m",
            "zonal_wind_10m",
            "meridional_wind_10m",
            "total_cloud_cover",
            "total_precipitation",
            "snowfall"
        ]]
    
    return df

def write_csv(n):
    df = weather_dataframe(n)
    with Timer(f"[n={n:03d}] Saving csv", n=df.shape[0]):
        df.to_csv(
            f"{CSV_PATH}/weather_hour{n}.csv",
            index=False,
            header=False,
            date_format="%Y-%m-%d %H:%M:%S"
        )
    return

if __name__ == "__main__":
    ds = xr.open_dataset("e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc")

    Parallel(n_jobs=24)(
        delayed(write_csv)(n)
        for n in tqdm(range(len(ds.time)))
    )
