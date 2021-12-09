# Environmental Data API

API for retrieving environmental data.

All data is interpolated to match the following constraints:
* Spatial Resolution: 0.083° &times; 0.083°
* Temporal Resolution: 3-hours interval
* All timestamps are assumed to be in UTC, hence no timezone information is expected.

## Download Data

Download environmental data matching the given parameters.

**URL**: `/request_env_data` 

**Method**: `GET`

**Headers**: 

* `Accept`: if `application/json` than response in json else html

**Parameters**:

* `date_lo`
  * **Required**: yes
  * **Type**: datetime-local
  * **Description**:
    * e.g. `2019-06-03T09:55`
    * `date_hi` - `date_lo` must not differ more than 10 days
    * Must meet `date_hi` > `date_lo`
* `date_hi`
  * **Required**: yes
  * **Type**: datetime-local
  * **Description**:
    * e.g. `2019-06-03T09:55`
    * `date_hi` - `date_lo` must not differ more than 10 days
    * Must meet `date_hi` > `date_lo`
* `lat_lo`
  * **Required**: yes
  * **Type**: number
  * **Description**:
    * e.g `52.0`
    * Between -90.0° and +90.0°
    * Will be rounded to 4 decimal places
    * Must meet `lat_hi` > `lat_lo`
* `lat_hi`
  * **Required**: yes
  * **Type**: number
  * **Description**:
    * e.g `52.0`
    * Between -90.0° and +90.0°
    * Will be rounded to 4 decimal places
    * Must meet `lat_hi` > `lat_lo`
* `lon_lo`
  * **Required**: yes
  * **Type**: number
  * **Description**:
    * e.g `7.0`
    * Between -180.0° and +180.0°
    * Will be rounded to 4 decimal places
    * Must meet `lon_hi` > `lon_lo`
* `lon_hi`
  * **Required**: yes
  * **Type**: number
  * **Description**:
    * e.g `7.0`
    * Between -180.0° and +180.0°
    * Will be rounded to 4 decimal places
    * Must meet `lon_hi` > `lon_lo`
* `format`
  * **Required**: yes
  * **Type**: string
  * **Description**:
    * Allowed values:
      * `csv`
      * `netcdf` 
* `GFS`
  * **Required**: no*
  * **Type**: comma separated list of strings or multiple times
  * **Description**:
    * Allowed values:
      * `Dewpoint_temperature_height_above_ground`: Dewpoint temperature @ Specified height level above ground
      * `Relative_humidity_height_above_ground`: Relative humidity @ Specified height level above ground
      * `Temperature_surface`: Temperature @ Ground or water surface'
      * `U-Component_Storm_Motion_height_above_ground_layer`: U-Component Storm Motion @ Specified height level above ground layer
      * `V-Component_Storm_Motion_height_above_ground_layer`: V-Component Storm Motion @ Specified height level above ground layer
      * `Wind_speed_gust_surface`: Wind speed (gust) @ Ground or water surface
      * `u-component_of_wind_maximum_wind`: u-component of wind @ Maximum wind level
      * `v-component_of_wind_maximum_wind`: v-component of wind @ Maximum wind level
* `Physical`
  * **Required**: no*
  * **Type**: comma separated list of strings or multiple times
  * **Description**:
    * Allowed values:
      * `bottomT`: Sea floor potential temperature
      * `mlotst`: Density ocean mixed layer thickness
      * `siconc`: Ice concentration
      * `sithick`: Sea ice thickness
      * `so`: Salinity
      * `thetao`: Potential Temperature
      * `uo`: Eastward velocity
      * `usi`: Sea ice eastward velocity
      * `vo`: Northward velocity
      * `vsi`: Sea ice northward velocity
      * `zos`: Sea surface height
* `Wave`
  * **Required**: no*
  * **Type**: comma separated list of strings or multiple times
  * **Description**:
    * Allowed values:
      * `VHM0_SW1`: sea surface primary swell wave significant height
      * `VHM0_WW`: sea surface wind wave significant height
      * `VHM0`: sea surface wave significant height
      * `VMDR_SW1`: sea surface primary swell wave from direction
      * `VMDR_SW2`: sea surface secondary swell wave from direction
      * `VMDR_WW`: sea surface wind wave from direction
      * `VMDR`: sea surface wave from direction
      * `VPED`: sea surface wave from direction at variance spectral density maximum
      * `VSDX`: sea surface wave stokes drift x velocity
      * `VSDY`: sea surface wave stokes drift y velocity
      * `VTM01_SW1`: sea surface primary swell wave mean period
      * `VTM01_SW2`: sea surface secondary swell wave mean period
      * `VTM01_WW`: sea surface wind wave mean period
      * `VTM02`: sea surface wave mean period from variance spectral density second frequency moment
      * `VTM10`: sea surface wave mean period from variance spectral density inverse frequency moment
      * `VTPK`: sea surface wave period at variance spectral density maximum
* `Wind`
  * **Required**: no*
  * **Type**: comma separated list of strings or multiple times
  * **Description**:
    * Allowed values:
      * `eastward_wind_rms`: eastward wind speed root mean square
      * `eastward_wind`: eastward wind speed
      * `northward_wind_rms`: northward wind speed root mean square
      * `northward_wind`: northward wind speed
      * `sampling_length`: sampling length
      * `surface_downward_eastward_stress`: eastward wind stress
      * `surface_downward_northward_stress`: northward wind stress
      * `surface_type`: flag with the following values: 0:ocean, 1:earth/ice
      * `wind_speed_rms`: wind speed root mean square
      * `wind_speed`: wind speed
      * `wind_stress_curl`: wind stress curl
      * `wind_stress_divergence`: wind stress divergence
      * `wind_stress`: wind stress
      * `wind_vector_curl`: wind vector curl
      * `wind_vector_divergence`: wind vector divergence

*: At least one value for GFS, Physical, Wave or Wind is required ([detailed description of the datasets][dataset_details]).



### Responses

#### Success

**Code**: 200

**Content-type**: as requested. `application/json` and `text/html` are supported

**Response**:

For environmental data requested in format `csv`.
* `link` denotes the location to download the data.
* `limit` shows the time from when the file should not be expected to be available anymore.

```json
{
  "limit": "2021-12-07T16:45:04+00:00",
  "link": "http://localhost:5000/EnvDataAPI/43954030-576c-11ec-a278-9361709dc712.csv"
}
```

For environmental data requested in format `netcdf`.

```json
{
  "limit": "2021-12-07T16:53:04+00:00",
  "link": "http://localhost:5000/EnvDataAPI/43954030-576c-11ec-a278-9361742dc712.nc"
}
```

#### Partly Successful

**Code**: 200

**Content-Type**: as requested. `application/json` and `text/html` are supported

**Response**:

For environmental data retrieval from GFS and another category.
* `error` informs about any error that happened when retrieving data of one category (GFS, Physical, Wave or Wind), but at least data retrieval for another category was successful.
  * The response code will be 500, if *all requested categories/variables* result in an error.
    In addition, `limit` and `link` will not be set.
* `limit` shows the time from when the file should not be expected to be available anymore.
* `link` denotes the location to download the data.


```json
{
  "error": "Error occurred while retrieving GFS data:  HTTPSConnectionPool(host='rda.ucar.edu', port=443): Max retries exceeded with url: /thredds/catalog/files/g/ds084.1/2019/20190601/catalog.xml (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7f8af2b3dbe0>: Failed to establish a new connection: [Errno 113] No route to host'))\n", 
  "limit": "2021-12-09T15:45:17+00:00", 
  "link": "http://localhost:5000/EnvDataAPI/3e2d0782-58f6-11ec-93f8-99c5f7a9768e.csv"
}
```

#### Error

For wrong parameter values, e.g. `format=txt`.

**Code**: 400

**Content-Type**: as requested. `application/json` and `text/html` are supported

**Response**:

```json
{
  "error": [
    "format parameter wrong/missing. Allowed values: csv, netcdf"
  ]
}
```

For failed retrieval for all selected variables.

**Code**: 500

**Content-Type**: as requested. `application/json` and `text/html` are supported

**Response**:

```json
{
  "error": "Error occurred while retrieving GFS data:  HTTPSConnectionPool(host='rda.ucar.edu', port=443): Max retries exceeded with url: /thredds/catalog/files/g/ds084.1/2019/20190601/catalog.xml (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7f8af2b63400>: Failed to establish a new connection: [Errno 113] No route to host'))\n\nError occurred: Empty dataset"
}
```

### Example cURL

```shell
curl -v -G -H "Accept: application/json" https://harvest.maridata.dev.52north.org/EnvDataAPI/request_env_data \
     -d 'date_lo=2019-06-02T03%3A44' -d 'date_hi=2019-06-03T09%3A55' \
     -d 'lat_lo=53.08' -d 'lat_hi=55.08' -d 'lon_lo=1.69' -d 'lon_hi=6.1' \
     -d 'Wave=VHM0_WW' -d 'Wave=VMDR_SW2' \
     -d 'GFS=U-Component_Storm_Motion_height_above_ground_layer' \
     -d 'GFS=V-Component_Storm_Motion_height_above_ground_layer' \
     -d 'format=csv'
```

## Merge Data

**URL**: `/merge_data`

**Method**: `POST`

**Headers**:

* `Accept`: if `application/json` than response in json else html

**Content-Type**: `multipart/form-data`

**Content**: 

* **Element**: `var`
  * **Description**: The requested variables, grouped by category. 
  * **Content-Type**: dictionary of lists
  * **Allowed Values**: see [Download Data](#download-data)
  * **Example**:
    ```json
    {
      "Wave": ["VHM0_WW", "VMDR", "VTM10", "VTPK", "VPED"],
      "Wind": ["surface_downward_eastward_stress", "wind_stress_divergence"],
      "GFS": [],
      "Physical":[]
    }
    ```

* **Element**: `col`
  * **Description**: Used for matching the used column lables in the CSV file to the required columns: time, lat, lon.
  * **Content-Type** dictionary
  * **Allowed Values**:
    * MUST contain the three keys: `time`, `lat`, and `lon`
    * Values MUST match the columns names of the `file`
  * **Example**:
    ```json
    {
      "time": "BaseDateTime",
      "lat": "LAT",
      "lon": "LON"
    }
    ```

* **Element**: `file`
  * **Description**: The file with timestamps and coordinates in WGS84 
  * **Content-Type**: `text/csv` with
    * `,` as column separator
    * `.` as decimal separator
    * `"` as quote character
  * **Columns**:
    * `time` using any label
      * string matching format `%Y-%m-%d %H:%M:%S`
      * Timezone is assumed to be UTC
      * Must meet `max(dateimte) - min(datetime) < 30 days`
    * `lat` using any label
      * number
      * Between -90.0 and +90.0
      * Must meet `|max(latitude) - min(latitude)| + |max(latitude) - min(latitude)| < 150.0`
    * `lon` using any label
      * number
      * Between -180.0 and +180.0
      * Must meet `|max(latitude) - min(latitude)| + |max(latitude) - min(latitude)| < 150.0`
  * **Example**:
    ```csv
    BaseDateTime,LAT,LON
    2021-12-02 00:03:00,33.63074,-118.33015
    2021-12-02 00:06:00,33.99982,-76.29557
    2021-12-02 00:09:00,39.14306,-76.40757
    ```
  
### Responses

Same as for [Download Data](#responses), except that the format of the data file will always be `text/csv`.

### Example cURL

```shell
curl -v -H "Accept: application/json" https://harvest.maridata.dev.52north.org/EnvDataAPI/merge_data \
     -F 'col={"time":"BaseDateTime","lat":"LAT","lon":"LON"}' \
     -F 'var={"Wave":["VHM0_WW","VMDR","VTM10","VTPK","VPED"],"Wind":[],"GFS":[],"Physical":[]}' \
     -F 'file=@test.csv'
```

with `test.csv`:

```csv
BaseDateTime,LAT,LON
2021-12-02 00:03:00,33.63074,-118.33015
2021-12-02 00:06:00,33.99982,-76.29557
2021-12-02 00:09:00,39.14306,-76.40757
```

## List of Error Messages

* "CSV file is not valid: Error occurred while appending env data: "
* "Error occurred while retrieving GFS data: <EXCEPTION>"
* "Error occurred while retrieving Physical data: <EXCEPTION>"
* "Error occurred while retrieving Wave data: <EXCEPTION>"
* "Error occurred while retrieving Wind data: <EXCEPTION>"
* "Error occurred: Empty dataset"
* "Error occurred: requested bbox ({0}° lat x {1}° lon x {2} days) is too large. Maximal bbox dimension ({3}° lat x {4}° lon x {5} days)." with
  * `0` := integer value of `lat_hi - lat_lo`
  * `1` := integer value of `lon_hi - lon_lo`
  * `2` := day value of `date_hi - date_lo`
  * `3` := currently `20` (see [configuration](./app.py#L42))
  * `4` := currently `20` (see [configuration](./app.py#L42))
  * `5` := currently `10` (see [configuration](./app.py#L42))
* "Missing mandatory parameter{0}: {1}" with
  * `0` := s if more than one parameter is missing
  * `1` := list of missing parameters
* "No variables are selected"
* "date_lo > date_hi"
* "format parameter wrong/missing. Allowed values: csv, netcdf"
* "lat_lo > lat_hi"
* "lon_lo > lon_hi"

[dataset_details]: https://docs.google.com/spreadsheets/d/1GxcBtnaAa2GQDwZibYFbWPXGi7BPpPdYLZwyetpsJOQ/edit#gid=0
