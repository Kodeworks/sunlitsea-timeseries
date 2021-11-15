import sys

from float_queries import FloatQueries

if __name__ == '__main__':
    # YOUR TOKEN HERE
    token = "<YOUR_TOKEN>"

    if len(sys.argv) == 2:
        token = sys.argv[1]

    print(f"Initialized with Token: {token}")

    # Initialize object used to query the database
    db = FloatQueries(token)

    # The returned data format can be set to either "csv" or "raw". It is "csv" by default
    db.result_format = "csv"

    # Create time-strings using helper methods
    start_time = db.to_query_date_string(day=1, month=11, year=2021, hour=4, minutes=0)
    end_time = db.to_query_date_string(day=1, month=11, year=2021, hour=4, minutes=1)
    print(f"time range 1: {start_time} - {end_time}")

    start_time_window = db.to_query_date_string(day=16, month=10, year=2021)
    end_time_window = db.to_query_date_string(day=17, month=10, year=2021)
    print(f"time range 2: {start_time_window} - {end_time_window}")

    # Example usage of the general query function
    custom_data1 = db.query_influx("proto3", ["f6_t1"], [], "-1h")

    custom_data2 = db.query_influx("testRig", ["f1_t1"], [], "1636715678", "1636716778")

    custom_data3 = db.query_influx("proto3", ["f18_t1"], ["rot_x", "rot_y"],
                                   start_time, end_time)

    custom_data4 = db.query_influx("proto3", ["f6_t1", "f18_t2"], [["rot_x", "rot_y"], ["a_x", "a_y"]],
                                   start_time, end_time)

    custom_data5 = db.query_influx("proto3", ["f6_t1", "f6_t2", "f18_t1", "f18_t2"],
                                   ["acc_x", "acc_y", "rot_x", "rot_y"],
                                   start_time_window, end_time_window, window_period="1h")

    custom_query6 = db.create_query("proto3", ["f6_t1", "f18_t1"], [["acc_x", "acc_y"], ["acc_y"]],
                                    start_time_window, end_time_window, window_period="1h")
    custom_data6 = db.send_query(custom_query6)

    # Example usage of specific queries
    temperatures = db.query_temperature("proto3", ["f6", "f18"], start_time_window, end_time_window, window_period="1h")

    humidities = db.query_humidity("proto3", ["f6", "f18"], start_time_window, end_time_window, window_period="1h")

    orientations = db.query_orientation("proto3", ["f6", "f18"], start_time_window, end_time_window, window_period="1h")

    IMU_datas = db.query_IMU_data("proto3", ["f6", "f18"], start_time_window, end_time_window, window_period="1h")

    IMU_datas = db.query_IMU_data("proto3", [], start_time_window, end_time_window, window_period="1h")

    float_data = db.query_floats("proto3", ["f18"], start_time_window, end_time_window, window_period="1h")

    # Return the query string before sending it
    temperature_query = db.create_temperature_query("proto3", ["f6", "f18"], start_time_window, end_time_window,
                                                    window_period="1h")
    print(f"{temperature_query}\n")
    temperatures = db.send_query(temperature_query)

    humidity_query = db.create_humidity_query("proto3", ["f6", "f18"], start_time_window, end_time_window,
                                              window_period="1h")
    print(f"{humidity_query}\n")
    humidities = db.send_query(humidity_query)

    orientation_query = db.create_orientation_query("proto3", ["f6", "f18"], start_time_window, end_time_window,
                                                    window_period="1h")
    print(f"{orientation_query}\n")
    orientations = db.send_query(orientation_query)

    IMU_data_query = db.create_IMU_data_query("proto3", ["f6", "f18"], start_time_window, end_time_window,
                                              window_period="1h")
    print(f"{IMU_data_query}\n")
    IMU_datas = db.send_query(IMU_data_query)

    IMU_data_query = db.create_IMU_data_query("proto3", [], start_time_window, end_time_window, window_period="1h")
    print(f"{IMU_data_query}\n")
    IMU_datas = db.send_query(IMU_data_query)

    float_data_query = db.create_floats_query("proto3", ["f18"], start_time_window, end_time_window, window_period="1h")
    print(f"{float_data_query}\n")
    float_data = db.send_query(float_data_query)

    # Write returned data to file
    with open('data/temperatures.csv', 'w') as f:
        for line in temperatures:
            f.write(f'{", ".join(line)}\n')
