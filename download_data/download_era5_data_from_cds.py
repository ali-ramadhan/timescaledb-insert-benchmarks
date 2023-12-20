import cdsapi

c = cdsapi.Client()

months = [f"{m:02d}" for m in range(1, 13)]
days = [f"{d:02d}" for d in range(1, 32)]
times = [f"{h:02d}:00" for h in range(0, 24)]
years = [str(y) for y in range(1940, 2023)]

def download_data_from_cds(var):
    for year in years:
        c.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": var,
                "year": year,
                "month": months,
                "day": days,
                "time": times,
            },
            f"{var}_{year}.nc")

if __name__ == "__main__":
    download_data_from_cds("total_precipitation")
    download_data_from_cds("snowfall")