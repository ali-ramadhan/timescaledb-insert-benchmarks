import os
import time
import xarray as xr
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

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

def latlon_to_location_id(lat, lon, dlat=0.25, dlon=0.25, min_lat=-90, min_lon=-179.75):
    if not (min_lat <= lat <= 90) or not (min_lon <= lon <= 180):
        raise ValueError(f"(lat, lon) = ({lat}, {lon}) is not a valid location!")
    
    n_lats = int(180 / dlat) + 1  # +1 to include both endpoints (poles)
    n_lons = int(360 / dlon)
    lat_index = int((lat - min_lat) / dlat)
    lon_index = int((lon - min_lon) / dlon)
    return lat_index * n_lons + lon_index + 1

def latlon_to_location_id_vectorized(lats, lons, dlat=0.25, dlon=0.25, min_lat=-90, min_lon=-179.75):
    n_lats = int(180 / dlat) + 1  # +1 to include both endpoints (poles)
    n_lons = int(360 / dlon)
    lat_indices = ((lats - min_lat) / dlat).astype(int)
    lon_indices = ((lons - min_lon) / dlon).astype(int)
    return lat_indices * n_lons + lon_indices + 1

def create_and_populate_locations_table(ds, engine):
    with Timer("Loading locations"):
        df = ds.isel(time=0).to_dataframe().reset_index()
        df.drop(columns=["VAR_2T", "time", "utc_date"], inplace=True)

        df.rename(columns={"longitude": "longitude_east"}, inplace=True)
        df["longitude"] = df["longitude_east"].apply(lambda x: x - 360 if x > 180 else x)
        df.drop(columns=["longitude_east"], inplace=True)

        with Timer("Computing locations index", n=df.shape[0]):
            df["id"] = latlon_to_location_id_vectorized(df.latitude, df.longitude)
            df.sort_values("id", inplace=True)
            df = df[["id", "longitude", "latitude"]]

    with Timer("Inserting locations into PostgreSQL", n=df.shape[0], units="inserts"):
        with engine.connect() as conn:
            conn.execute(text("""--sql
                create table if not exists locations (
                    id int not null primary key,
                    latitude float4 not null,
                    longitude float4 not null
                );
            """))

            conn.commit()

            df.to_sql("locations", engine, index=False, if_exists="append", method="multi")

            conn.commit()

if __name__ == "__main__":
    ds = xr.open_dataset("e5.oper.an.sfc.128_167_2t.ll025sc.1995030100_1995033123.nc")
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB_NAME}")
    create_and_populate_locations_table(ds, engine)
