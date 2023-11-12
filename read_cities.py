import pandas as pd

# See: https://download.geonames.org/export/dump/
column_names = [
    "geoname_id", # integer id of record in geonames database
    "name", # name of geographical point (utf8) varchar(200)
    "ascii_name", # name of geographical point in plain ascii characters, varchar(200)
    "alternate_names", # alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
    "latitude", # latitude in decimal degrees (wgs84)
    "longitude", # longitude in decimal degrees (wgs84)
    "feature_class", # see http://www.geonames.org/export/codes.html, char(1)
    "feature_code", # see http://www.geonames.org/export/codes.html, varchar(10)
    "country_code", # ISO-3166 2-letter country code, 2 characters
    "cc2", # alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
    "admin1_code", # fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
    "admin2_code", # code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
    "admin3_code", # code for third level administrative division, varchar(20)
    "admin4_code", # code for fourth level administrative division, varchar(20)
    "population", # bigint (8 byte int)
    "elevation", # in meters, integer
    "elevation_digital", # digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
    "timezone", # the iana timezone id (see file timeZone.txt) varchar(40)
    "modification_date" # date of last modification in yyyy-MM-dd format
]

columns_to_keep = [
    "name",
    "latitude",
    "longitude",
    "country_code",
    "population",
    "elevation",
    "elevation_digital"
]

df = pd.read_csv("cities500.txt", delimiter='\t', header=0, names=column_names, usecols=columns_to_keep)
