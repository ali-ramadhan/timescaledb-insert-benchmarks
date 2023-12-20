import subprocess
import calendar

def download_file(url):
    try:
        subprocess.run(["wget", "--no-clobber", url], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error downloading {url}. Return code: {e.returncode}")

# See: https://rda.ucar.edu/OS/web/datasets/ds633.0/docs/ds633.0.e5.oper.an.sfc.grib1.table.web.txt
parameter_ids = {
    "tcc": 164,
    "10u": 165,
    "10v": 166,
    "2t": 167
}

def era5_ncar_url(var, year, month):
    base_url = "https://data.rda.ucar.edu/ds633.0/e5.oper.an.sfc"

    _, end_day = calendar.monthrange(year, month)  # Fetch the last day of the month
    pid = parameter_ids[var]

    url = f"{base_url}/{year}{month:02d}/e5.oper.an.sfc.128_{pid}_{var}.ll025sc.{year}{month:02d}0100_{year}{month:02d}{end_day}23.nc"

    return url

years = range(1940, 2023)  # ERA5 data is available from 1940 to present

def download_data_from_ncar(var):
    for year in years:
        for month in range(1, 13):
            url = era5_ncar_url(var, year, month)
            download_file(url)

if __name__ == "__main__":
    download_data_from_ncar("tcc")
    download_data_from_ncar("2t")
    download_data_from_ncar("10u")
    download_data_from_ncar("10v")
