from influxdb_client.client import influxdb_client


class InfluxBase:
    def __init__(self, token, org="Kodeworks", url="https://ts.sunlitsea.no/"):
        self.org = org
        self.url = url
        self.token = token
        self.influxClient = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.influxClient.query_api()
        self.result_format = "standard"  # Accepted values: standard, pandas, csv, raw
        self.drop_unused_columns = True

    @staticmethod
    def get_independent_filter_strings(measurements, fields):
        measurement_query = " or ".join(f'r._measurement == "{measurement}"' for measurement in measurements)
        field_query = " or ".join(f'r._field == "{field}"' for field in fields)
        return measurement_query, field_query

    @staticmethod
    def get_dependent_filter_string(measurements, fields):
        measurement_queries = list(f'r._measurement == "{measurement}"' for measurement in measurements)
        field_queries = []
        for i in range(len(measurements)):
            if len(fields[i]) != 0:
                field_queries.append(f""" and ({" or ".join(f'r._field == "{field}"' for field in fields[i])})""")
            else:
                field_queries.append('')

        filter_queries = list(f'{m_query}{f_query}' for m_query, f_query in zip(measurement_queries, field_queries))
        filter_string = " or ".join(filter_queries)
        return filter_string

    @staticmethod
    def create_query(bucket, measurements, fields, start_time, end_time="", window_period="", drop_columns=True):
        end_time_string = ""
        if end_time:
            end_time_string += f", stop: {end_time}"

        query_stub = f'from(bucket: "{bucket}")\n' \
                     f'|> range(start: {start_time}{end_time_string})\n'

        query_filter = ""

        if len(fields) == 0:
            (measurement_query, _) = InfluxBase.get_independent_filter_strings(measurements, fields)
            query_filter += f'|> filter(fn: (r) => {measurement_query})\n'
        elif not isinstance(fields[0], list):
            (measurement_query, field_query) = InfluxBase.get_independent_filter_strings(measurements, fields)
            query_filter += f'|> filter(fn: (r) => {measurement_query})\n'
            query_filter += f'|> filter(fn: (r) => {field_query})\n'
        else:
            query_filter += f'|> filter(fn: (r) => {InfluxBase.get_dependent_filter_string(measurements, fields)})\n'

        query_drop_columns = ""
        if drop_columns:
            query_drop_columns += f'|> drop(columns: ["_start", "_stop"])\n'

        query_aggregate = ""
        if window_period:
            query_aggregate += f'|> aggregateWindow(every: {window_period}, fn: mean, createEmpty: false)\n' \
                               f'{query_drop_columns}' \
                               f'|> yield(name: "mean")'
        else:
            query_aggregate += query_drop_columns

        return query_stub + query_filter + query_aggregate

    def send_query(self, query):
        if self.result_format == "pandas" or self.result_format == "standard":
            return self.query_api.query_data_frame(query, org=self.org)
        if self.result_format == "csv":
            return self.query_api.query_csv(query, org=self.org)
        elif self.result_format == "raw":
            return self.query_api.query_raw(query, org=self.org)
        else:
            return self.query_api.query(query, org=self.org)

    def query_influx(self, bucket, measurements, fields, start_time, end_time="", window_period=""):
        query = self.create_query(bucket, measurements, fields, start_time, end_time, window_period,
                                  self.drop_unused_columns)
        return self.send_query(query)
