import math
import numpy as np
import pandas as pd

from influx_base import InfluxBase


class FloatQueries(InfluxBase):
    number_of_humidity_temperature_sensors = 30
    IMU_data_identifier = "t1"
    orientation_identifier = "t2"
    temperature_identifier = "t3"

    installation_id_to_float_ids = {
        "proto3": ["f6", "f18"],
        "testRig": ["f1", "f2", "f3", "f5"]
    }

    def __init__(self, token, org="Kodeworks", url="https://ts.sunlitsea.no/"):
        super().__init__(token, org, url)
        self.calculate_solar_angles = True

    # Helper methods
    ######################################################
    def float_id_to_installation_id(self, float_id):
        for installation_id, float_ids in self.installation_id_to_float_ids.items():
            if float_id in float_ids:
                return installation_id

        return "-1"

    @staticmethod
    def to_query_date_string(day, month, year, hour=0, minutes=0, seconds=0):
        return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minutes:02d}:{seconds:02d}Z"

    @staticmethod
    def custom_sort_columns(arr):
        with_digits: list = list(element for element in arr if any(map(str.isdigit, element)))
        without_digits: list = list(element for element in arr if element not in with_digits)

        # Sort by digits first, then by the first letter
        with_digits.sort(key=lambda x: int(x[1:]))
        with_digits.sort(key=lambda x: x[0])

        return np.array(without_digits + with_digits)

    @staticmethod
    def pitch_roll_to_azimuth_tilt(pitch, roll):
        azimuth = - math.atan2(math.sin(roll), math.tan(pitch))
        tilt = math.atan2(math.sqrt(math.sin(pitch) ** 2 + math.cos(pitch) ** 2 * math.sin(roll) ** 2),
                          math.cos(pitch) * math.cos(roll))
        return azimuth, tilt

    def convert_data_frame(self, df: pd.DataFrame):
        # Converts to the following columns:
        # Unix time, installation_id, string_id, Float_id, orientation_field1, ... orientation_fieldN,
        # humidityTempField1, ... humidityTempFieldM
        if df.empty:
            return df

        df.rename(columns={c: c.replace("_", "") for c in df.columns}, inplace=True)

        df.sort_values(["time", "measurement"], inplace=True)

        # Remove measurement series names, keep float id
        df['measurement'] = df['measurement'].apply(lambda x: x.split("_")[0])
        df.rename(columns={"measurement": "float_id"}, inplace=True)

        measurement_names: np.array = df.get("field").unique()
        convert_angles = self.calculate_solar_angles and all(x in measurement_names for x in ['a_x', 'a_y'])

        if convert_angles:
            measurement_names = list(measurement_names) + ['azimuth', 'tilt']
        measurement_names = self.custom_sort_columns(measurement_names)

        field_names = ["time", "installation_id", "string_id", "float_id"] + measurement_names.tolist()

        df_grouped = df.groupby(by=["time", "float_id"])

        # Initialize a numpy array to hold the transformed data
        data_types = ['i8', 'U8', 'i4', 'i4'] + np.full(shape=measurement_names.shape[0], fill_value='f8').tolist()
        dtype = list(zip(field_names, data_types))
        transformed_arr = np.full(shape=(df_grouped.ngroups,), fill_value=np.nan, dtype=dtype)

        row = 0
        for name, group in df_grouped:
            head = group.iloc[0]
            unix_time = (head['time'] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
            transformed_arr[row]['time'] = unix_time
            transformed_arr[row]['installation_id'] = self.float_id_to_installation_id(
                head['float_id'])
            transformed_arr[row]['string_id'] = 0
            transformed_arr[row]['float_id'] = ''.join(filter(str.isdigit, head['float_id']))

            for idx, entry in group.iterrows():
                transformed_arr[row][entry['field']] = entry['value']

            if convert_angles:
                azimuth, tilt = self.pitch_roll_to_azimuth_tilt(transformed_arr[row]['a_x'],
                                                                transformed_arr[row]['a_y'])
                transformed_arr['azimuth'] = azimuth
                transformed_arr['tilt'] = tilt

            row += 1
        return pd.DataFrame(data=transformed_arr, columns=field_names)

    # Create queries
    ######################################################
    def create_temperature_query(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        if not float_ids:
            float_ids = self.installation_id_to_float_ids[installation_id]

        measurements = list(f"{float_id}_{self.temperature_identifier}" for float_id in float_ids)
        fields = list(f"t{numb}" for numb in range(self.number_of_humidity_temperature_sensors))

        return self.create_query(installation_id, measurements, fields,
                                 start_time, end_time, window_period)

    def create_humidity_query(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        if not float_ids:
            float_ids = self.installation_id_to_float_ids[installation_id]

        measurements = list(f"{float_id}_{self.temperature_identifier}" for float_id in float_ids)
        fields = list(f"h{numb}" for numb in range(self.number_of_humidity_temperature_sensors))

        return self.create_query(installation_id, measurements, fields,
                                 start_time, end_time, window_period)

    def create_orientation_query(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        if not float_ids:
            float_ids = self.installation_id_to_float_ids[installation_id]
        measurements = list(f"{float_id}_{self.orientation_identifier}" for float_id in float_ids)

        return self.create_query(installation_id, measurements, [],
                                 start_time, end_time, window_period)

    def create_IMU_data_query(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        if not float_ids:
            float_ids = self.installation_id_to_float_ids[installation_id]

        measurements = list(f"{float_id}_{self.IMU_data_identifier}" for float_id in float_ids)

        return self.create_query(installation_id, measurements, [],
                                 start_time, end_time, window_period)

    def create_floats_query(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        if not float_ids:
            float_ids = self.installation_id_to_float_ids[installation_id]

        measurements = list(f"{float_id}_{self.IMU_data_identifier}" for float_id in float_ids)
        measurements += list(f"{float_id}_{self.orientation_identifier}" for float_id in float_ids)
        measurements += list(f"{float_id}_{self.temperature_identifier}" for float_id in float_ids)

        return self.create_query(installation_id, measurements, [],
                                 start_time, end_time, window_period)

    # Create and send queries
    ######################################################
    def send_query(self, query):
        result = super().send_query(query)
        if self.result_format == "standard":
            return self.convert_data_frame(result)

        return result

    def query_temperature(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        query = self.create_temperature_query(installation_id, float_ids, start_time, end_time, window_period)
        return self.send_query(query)

    def query_humidity(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        query = self.create_humidity_query(installation_id, float_ids, start_time, end_time, window_period)
        return self.send_query(query)

    def query_orientation(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        query = self.create_orientation_query(installation_id, float_ids, start_time, end_time, window_period)
        return self.send_query(query)

    def query_IMU_data(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        query = self.create_IMU_data_query(installation_id, float_ids, start_time, end_time, window_period)
        return self.send_query(query)

    def query_floats(self, installation_id, float_ids, start_time, end_time="", window_period=""):
        query = self.create_floats_query(installation_id, float_ids, start_time, end_time, window_period)
        return self.send_query(query)
