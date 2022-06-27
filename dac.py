import os
import sys
from argparse import ArgumentParser, BooleanOptionalAction
from datetime import datetime

import numpy as np
import pandas as pd

from float_queries import FloatQueries

parser = ArgumentParser()
parser.add_argument("--token", help="Access token to influxDB database. Get Sunlitsea Token from eirik@kodeworks.no",
                    required=True)
parser.add_argument("--output_dir",
                    help="Folder where downloaded datasets are saved", required=True)
parser.add_argument("--timestamp_file", default="dataset_download_times.csv",
                    help="Optional. Path to the file specifying timestamps to download. " +
                         "Defaults to dataset_download_times.csv",
                    required=False)

parser.add_argument("--debug", action=BooleanOptionalAction,
                    help="Prints each query that gets sent to the database",
                    required=False)

args = parser.parse_args()

print(f"Initialized with Token: {args.token}")

args.output_dir = os.path.normpath(args.output_dir)
if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)

print(f"Writing data to {args.output_dir}\nReading times from {args.timestamp_file}")
# Initialize object used to query the database
db = FloatQueries(args.token)
cropped_times = {}

df = pd.read_csv(args.timestamp_file, sep=", ", engine='python')
for time in "start_time", "end_time":
    df[time] = np.asarray(
        list(datetime.strptime(time_string, "%H:%M:%S %d.%m.%Y") for time_string in
             df[time]))

for _, row in df.iterrows():
    cropped_times[row["dataset_name"]] = (
        db.datetime_to_query_date_string(row["start_time"]),
        db.datetime_to_query_date_string(row["end_time"])
    )

for key, val in cropped_times.items():
    print(f"{key}:\t\t{val}")

# Variables that modify the default behaviour of the queries
db.result_format = "standard"  # Options: standard, csv, pandas, raw. Default: standard
db.drop_unused_columns = True  # Doesn't request the _start and _stop columns if set to True
# db.calculate_solar_angles = True  # Includes Azimuth and Tilt in the dataset when set to True
db.calculate_solar_angles = True  # Includes Azimuth and Tilt in the dataset when set to True

for measurement_name, (start_time, end_time) in cropped_times.items():
    query = db.create_floats_query("DAC1", [], start_time, end_time)

    if args.debug:
        print(f"\n{query}\n")

    dac = db.query_floats("DAC1", [], start_time, end_time)
    file_path = os.path.join(args.output_dir, f"orientation_{measurement_name}.csv")
    dac.to_csv(file_path)
    print(f"Saved to {file_path}")
