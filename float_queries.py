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

    # Helper methods
    ######################################################
    @staticmethod
    def query_result_to_csv_file(result):
        return result

    @staticmethod
    def to_query_date_string(day, month, year, hour=0, minutes=0, seconds=0):
        return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minutes:02d}:{seconds:02d}Z"

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
