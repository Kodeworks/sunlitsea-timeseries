# Sunlit Sea TimeSeries
[comment]: <> (TODO: Give project a title)
Sunlit Sea TimeSeries is used to fetch data collected from installations of floats operated by Sunlit Sea. Each Float 
reports its orientation (position and rotation), IMU/gyroscope measurements as well as temperature and humidity arrays.
More details about using influx can be found in the [Guide To Influx (Sunlit Sea)](https://docs.google.com/document/d/e/2PACX-1vSYEFJ291IO4_HAji4TT_fgdnneMJIjE5vHCu934fofx2QY752pdC2pkvqhZ3nPcr1TmYUUOmBhQKra/pub).

## Installation
Download Python from [Python's active releases](https://www.python.org/downloads/). The project has been written and 
tested using Python 3.9.5.

`python -m pip install virtualenv`

`virtualenv venv`

`venv/scripts/activate`

`python -m pip install -r requirements.txt`


## Usage
The code base consists of a base class ```InfluxBase``` which can be used to send general Influx queries, as well as 
```floatQueries``` which is used for commonly used float-specific queries. ```floatQueries``` inherits the basic query 
functions from 
```InfluxBase```. Examples on how to use the classes can be found in `example_usage.py`. The examples can be ran with 
the following command:

`python example_usage.py <YOUR_INFLUX_TOKEN>`

a personal influx API token can be found on the [influx dashboard](https://ts.sunlitsea.no/) under `data > tokens`. 
See **contact** if you are interested in access to the database.

### Valid Time Formats
Each query consists of a start time and an optional end time which defaults to ```now()```. both the start time and 
end time accept relative durations, absolute times, or integers representing the Unix time in seconds. A relative time 
can be obtained by using the helper function ```to_query_date_string(day, month, year, hour, minutes, seconds)```. 
The hour, minutes and seconds are optional and default to zero. 

Examples of relative durations: ```-1h```, ```-3m```, ```-10d```

Example of relative times: ```2021-10-17T00:00:00Z```

Example of Unix timestamp: ```1567029600```

## Queries
A query can be sent with the general query function or one of the float-specific queries. Their formats are shown below.
The end time and window period are both optional.
```python
db = FloatQueries("<YOUR_TOKEN>")
# General query
result = db.query_influx(self, bucket, measurements, fields, start_time, end_time="",  window_period="")

# Float-specific queries
result = db.query_temperature(installation_id, float_ids, start_time, end_time="", window_period="")

result = db.query_humidity(installation_id, float_ids, start_time, end_time="", window_period="")

result = db.query_orientation(installation_id, float_ids, start_time, end_time="", window_period="")

result = db.query_IMU_data(installation_id, float_ids, start_time, end_time="", window_period="")

result = db.query_floats(installation_id, float_ids, start_time, end_time="", window_period="")
```
* The `installation_id` should be a valid installation identifier such as _proto3_ or _testRig_ and corresponds to 
Bucket names in influx. 
* In the float-specific queries The `float_ids` identify which floats within the installation you are requesting data 
from. If left empty, all floats in the installation are selected. 
* the `measurement` field in the general query loosely corresponds to the float_ids, but also contains identifiers for
which types of data are being selected. They are on the form `f#floatId_t#measurementIdentifier` where
  * `t1`: IMU data (`acc_x, acc_y, acc_z, rot_x, rot_y, rot_z`)
  * `t2`: Orientation data (`a_x, a_y, h`)
  * `t3`: Humidity and Temperature array (`t0-t29, h0-h29`)
* `fields` represents the desired data names, as listed in the parentheses above. It can either be a single list
`[data1, data2, data3]` in which case it will attempt to choose those fields from each selected measurements. 
Alternatively it may be a 2d list which contains an array of fields to select for each measurement; 
`[[data1, data3], [data1]]`. The number of arrays must then match the number of measurements. If left empty, 
all fields in the measurement category are returned.
* The `window_period` represents the interval between returned values in a series. 
This is done by taking the mean over the interval before it is sent.

If one wishes to view or change the queries before they are sent, each query function has a corresponding function that
begins with `create_`. The returned query can then be sent using `db.send_query(query)`. A code snippet which corresponds
to the one above is shown below.

```python
db = FloatQueries("<YOUR_TOKEN>")
# General query
query = db.create_query(bucket, measurements, fields, start_time, end_time="",  window_period="")
result = db.send(query)

# Float-specific queries
query = db.create_temperature_query(installation_id, float_ids, start_time, end_time="", window_period="")
result = db.send(query)

query = db.create_humidity_query(installation_id, float_ids, start_time, end_time="", window_period="")
result = db.send(query)

query = db.create_orientation_query(installation_id, float_ids, start_time, end_time="", window_period="")
result = db.send(query)

query = db.create_IMU_data_query(installation_id, float_ids, start_time, end_time="", window_period="")
result = db.send(query)

query = db.create_floats_query(installation_id, float_ids, start_time, end_time="", window_period="")
result = db.send(query)
```

## Contact
Requests for access to the influx database can be directed at [bjorn@sunlitsea.no](mailto:bjorn@sunlitsea.no).
Questions regarding this code or influx can be sent to [ferdy@kodeworks.no](mailto:ferdy@kodeworks.no).