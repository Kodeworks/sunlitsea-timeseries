import os
import sys

from float_queries import FloatQueries

# YOUR TOKEN HERE
token = "<YOUR_TOKEN>"

if len(sys.argv) == 2:
    token = sys.argv[1]

print(f"Initialized with Token: {token}")

file_path = "STT/cropped_float_measurements"

# Initialize object used to query the database
db = FloatQueries(token)
cropped_times = {
    "log14": (db.to_query_date_string(17, 3, 2022, hour=14, minutes=0),
              db.to_query_date_string(17, 3, 2022, hour=14, minutes=4)),
    "log15": (db.to_query_date_string(17, 3, 2022, hour=14, minutes=14),
              db.to_query_date_string(17, 3, 2022, hour=14, minutes=23)),

    "log16": (db.to_query_date_string(18, 3, 2022, hour=8, minutes=40),
              db.to_query_date_string(18, 3, 2022, hour=8, minutes=49)),
    "log17": (db.to_query_date_string(18, 3, 2022, hour=9, minutes=24),
              db.to_query_date_string(18, 3, 2022, hour=9, minutes=28)),
    "log18": (db.to_query_date_string(18, 3, 2022, hour=9, minutes=48),
              db.to_query_date_string(18, 3, 2022, hour=9, minutes=52)),
    "log19": (db.to_query_date_string(18, 3, 2022, hour=10, minutes=2),
              db.to_query_date_string(18, 3, 2022, hour=10, minutes=5)),
    "log20": (db.to_query_date_string(18, 3, 2022, hour=10, minutes=17),
              db.to_query_date_string(18, 3, 2022, hour=10, minutes=20)),
    "log21": (db.to_query_date_string(18, 3, 2022, hour=10, minutes=32),
              db.to_query_date_string(18, 3, 2022, hour=10, minutes=38)),

    "log22": (db.to_query_date_string(21, 3, 2022, hour=14, minutes=37),
              db.to_query_date_string(21, 3, 2022, hour=14, minutes=46)),
    "log23": (db.to_query_date_string(21, 3, 2022, hour=14, minutes=54),
              db.to_query_date_string(21, 3, 2022, hour=14, minutes=57)),

    "log24": (db.to_query_date_string(22, 3, 2022, hour=10, minutes=25),
              db.to_query_date_string(22, 3, 2022, hour=10, minutes=36)),
    "log25": (db.to_query_date_string(22, 3, 2022, hour=10, minutes=52),
              db.to_query_date_string(22, 3, 2022, hour=11, minutes=0)),
}
# Variables that modify the default behaviour of the queries
db.result_format = "standard"  # Options: standard, csv, pandas, raw. Default: standard
db.drop_unused_columns = True  # Doesn't request the _start and _stop columns if set to True
db.calculate_solar_angles = True  # Includes Azimuth and Tilt in the dataset when set to True
if not os.path.exists(file_path):
    os.makedirs(file_path)

for measurement_name, (start_time, end_time) in cropped_times.items():
    query = db.create_floats_query("STT", [], start_time, end_time)
    print(query)
    #stt = db.query_floats("STT", [], start_time, end_time)
    #stt.to_csv(f"{file_path}/float-{measurement_name}.csv")
