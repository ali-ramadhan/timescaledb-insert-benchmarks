from tqdm import tqdm
from sqlalchemy import text
from weather_write_csv import weather_dataframe
from utils import Timer, get_engine

def insert_weather_data(n):
    df = weather_dataframe(n)
    df = df.head(10000)
    with Timer(f"Inserting data row-by-row", n=len(df.index), units="inserts"):
        engine = get_engine()
        # df.to_sql("weather", engine, if_exists="append", index=False)

        with engine.connect() as conn:
            for index, row in df.iterrows():
                insert_query = """
                    INSERT INTO weather (time, location_id, latitude, longitude, temperature_2m, zonal_wind_10m, meridional_wind_10m, total_cloud_cover, total_precipitation, snowfall) 
                    VALUES (:time, :location_id, :latitude, :longitude, :temperature_2m, :zonal_wind_10m, :meridional_wind_10m, :total_cloud_cover, :total_precipitation, :snowfall)
                """

                conn.execute(text(insert_query), {
                    "time": row["time"],
                    "location_id": row["location_id"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "temperature_2m": row["temperature_2m"],
                    "zonal_wind_10m": row["zonal_wind_10m"],
                    "meridional_wind_10m": row["meridional_wind_10m"],
                    "total_cloud_cover": row["total_cloud_cover"],
                    "total_precipitation": row["total_precipitation"],
                    "snowfall": row["snowfall"]
                })


    return

if __name__ == "__main__":
    insert_weather_data(0)
