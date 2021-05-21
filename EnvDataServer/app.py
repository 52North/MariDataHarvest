import os
import threading
from pathlib import Path
import logging
from flask import Flask, render_template, request, send_from_directory, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from utilities.helper_functions import str_to_date_min
from EnvironmentalData.weather import get_global_wave, get_global_wind, get_GFS, get_global_phy_daily
from datetime import timedelta, datetime
import xarray as xr
import uuid
import numpy as np
import time
import pytz
from waitress import serve
from paste.translogger import TransLogger

logger = logging.getLogger('EnvDataServer.app')

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["70 per hour"]
)

# global variables
# TODO move to a config file
delete_file_queue = dict()
# in Minutes
FILE_LIFE_SPAN = 120
# in degrees
spatial_interpolation_rate = 0.083
# in hours
temporal_interpolation_rate = 3
# max bounding box
max_lat, max_lon, max_days = 20, 20, 10


def remove_files():
    while True:
        _deep_copy = delete_file_queue.copy()
        for f_path, created_time in _deep_copy.items():
            if (datetime.now() - created_time).seconds > FILE_LIFE_SPAN * 60:
                Path(f_path).unlink(missing_ok=True)
                del delete_file_queue[f_path]
                logger.debug('Deleting expired file %s ' % f_path)
        time.sleep(10)


dir_cleaner_thread = threading.Thread(target=remove_files)
dir_cleaner_thread.daemon = True
dir_cleaner_thread.start()


def parse_requested_var(args):
    wave, wind, gfs, phy = [], [], [], []
    for var in args:
        var = str(var)
        if var.startswith('var'):
            _, name, varName = var.split('-$$-')
            if name == 'Wave':
                wave.append(varName)
            elif name == 'Wind':
                wind.append(varName)
            elif name == 'GFS':
                gfs.append(varName)
            elif name == 'Physical':
                phy.append(varName)
    return wave, wind, gfs, phy


def create_csv(df, metadata_dict, file_path):
    csv_str = df.to_csv()
    csv_coma_line = csv_str[:csv_str.find('\n')].count(',') * ',' + '\n'
    csv_str = csv_coma_line.join(metadata_dict.values()) + csv_coma_line + csv_str
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        f.write(csv_str)


@app.route('/EnvDataAPI/request_env_data')
@limiter.limit("1/10second")
def request_env_data():
    logger.debug(request)
    dataset_list = []
    wave, wind, gfs, phy = parse_requested_var(request.args)
    date_lo = str_to_date_min(request.args.get('date_lo')) - timedelta(hours=temporal_interpolation_rate)
    date_hi = str_to_date_min(request.args.get('date_hi')) + timedelta(hours=temporal_interpolation_rate)
    lat_lo = float(request.args.get('lat_lo')) - spatial_interpolation_rate
    lat_hi = float(request.args.get('lat_hi')) + spatial_interpolation_rate
    lon_lo = float(request.args.get('lon_lo')) - spatial_interpolation_rate
    lon_hi = float(request.args.get('lon_hi')) + spatial_interpolation_rate
    #
    #   rounding coordinates to reasonable accuracy
    #
    #   see https://gis.stackexchange.com/a/208739
    #
    lat_lo = round(lat_lo, 4)
    lat_hi = round(lat_hi, 4)
    lon_hi = round(lon_hi, 4)
    lon_lo = round(lon_lo, 4)
    logger.debug("Rounded coordinates to 4 decimal places: Lat: [{}, {}]; Lon: [{}, {}]".format(
        lat_lo, lat_hi, lon_lo, lon_hi
    ))
    data_format = request.args.get('format')
    if lat_lo > lat_hi:
        logger.debug('Error: lat_lo > lat_hi')
        return render_template('error.html', error='Error: lat_lo > lat_hi')
    if lon_lo > lon_hi:
        logger.debug('Error: lon_lo > lon_hi ')
        return render_template('error.html', error='Error: lon_lo > lon_hi')
    if date_lo > date_hi:
        logger.debug('Error: date_lo > date_hi')
        return render_template('error.html', error='Error: date_lo > date_hi')
    if len(wave + wind + gfs + phy) == 0:
        logger.debug('Error: No variables are selected')
        return render_template('error.html', error='Error: No variables are selected')
    errorString = ''

    lat_interpolation = list(np.arange(lat_lo, lat_hi, spatial_interpolation_rate))
    lon_interpolation = list(np.arange(lon_lo, lon_hi, spatial_interpolation_rate))
    temporal_interpolation = [date_lo + timedelta(hours=hours) for hours in
                              range(0, (date_hi - date_lo).days * 24 + (date_hi - date_lo).seconds // 3600,
                                    temporal_interpolation_rate)]

    def rescale_dataset(dataset: xr.Dataset, gfs_flag=False) -> xr.Dataset:
        if gfs_flag:
            return dataset.interp(
                latitude=xr.DataArray(lat_interpolation, coords=[lat_interpolation], dims=["latitude"]),
                longitude=((xr.DataArray(lon_interpolation, coords=[lon_interpolation],
                                         dims=["longitude"]) + 180) % 360) + 180,
                time=xr.DataArray(temporal_interpolation, coords=[temporal_interpolation], dims=["time"]))
        return dataset.interp(
            latitude=xr.DataArray(lat_interpolation, coords=[lat_interpolation], dims=["latitude"]),
            longitude=xr.DataArray(lon_interpolation, coords=[lon_interpolation], dims=["longitude"]),
            time=xr.DataArray(temporal_interpolation, coords=[temporal_interpolation], dims=["time"]))

    if int(lat_hi - lat_lo) > max_lat or int(lon_hi - lon_lo) > max_lon or (date_hi - date_lo).days > max_days:
        error = 'Error occurred: requested bbox ({0}° lat x {1}° lon x {2} days) is too large. Maximal bbox dimension ({3}° lat x {4}° lon x {5} days).'.format(
            int(lat_hi - lat_lo),
            int(lon_hi - lon_lo), (date_hi - date_lo).days, max_lat, max_lon, max_days)
        logger.debug(error)
        return render_template('error.html', error=error)

    if len(wave) > 0:
        try:
            with get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0] as wave_ds:
                dataset_list.append(rescale_dataset(wave_ds))
                wave = [var for var in wave if var in list(wave_ds.keys())]
        except Exception as e:
            wave = []
            errorString += 'Error occurred while retrieving Wave data:  ' + str(e) + '\n'

    if len(wind) > 0:
        try:
            with get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].rename(
                    {'lat': 'latitude', 'lon': 'longitude'}) as dataset_wind:
                dataset_list.append(rescale_dataset(dataset_wind))
                wind = [var for var in wind if var in list(dataset_wind.keys())]
        except Exception as e:
            wind = []
            errorString += 'Error occurred while retrieving Wind data:  ' + str(e) + '\n'

    if len(phy) > 0:
        try:
            with get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].squeeze() as dataset_phy:
                dataset_list.append(rescale_dataset(dataset_phy))
                phy = [var for var in phy if var in list(dataset_phy.keys())]
        except Exception as e:
            phy = []
            errorString += 'Error occurred while retrieving Physical data:  ' + str(e) + '\n'

    if len(gfs) > 0:
        try:
            dataset_gfs, gfs_type = get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
            if gfs_type == 'gfs_50':
                dataset_gfs = dataset_gfs.rename({'lat': 'latitude', 'lon': 'longitude'})
            dataset_list.append(rescale_dataset(dataset_gfs, gfs_flag=True))
            gfs = [var for var in gfs if var in list(dataset_gfs.keys())]
        except Exception as e:
            gfs = []
            errorString += 'Error occurred while retrieving GFS data:  ' + str(e) + '\n'

    combined = xr.combine_by_coords(dataset_list, combine_attrs='drop', compat='override')[wave + wind + phy + gfs]
    if len(combined) == 0:
        logger.debug(errorString + 'Error occurred: Empty dataset')
        return render_template('error.html', error=errorString + 'Error occurred: Empty dataset')
    dir_path = Path(Path(__file__).parent, 'download')
    dir_path.mkdir(exist_ok=True)
    metadata_dict = dict(
        timeRange='Time range: %s to %s' % (str(date_lo), str(date_hi)),
        lon_extent='Longitude extent: %.2f to %.2f' % (lon_lo, lon_hi),
        lat_extent='Latitude extent: %.2f to %.2f' % (lat_lo, lat_hi),
        spatial_res='Spatial Resolution 0.083deg x 0.083deg',
        temporal_res='Temporal Resolution 3-hours interval',
        credit_CMEMS='Credit (Wave-Wind-Physical): E.U. Copernicus Marine Service Information (CMEMS)',
        credit_GFS='Credit (GFS): National Centers for Environmental Prediction/National Weather Service/NOAA',
        created='Accessed on %s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        errors='Error(s): ' + errorString.replace(',', ' ').replace('\n', ' ')
    )
    if data_format == 'csv':
        file_path = Path(dir_path, str(uuid.uuid1()) + '.csv')
        create_csv(combined.to_dataframe(), metadata_dict, file_path)
    elif data_format == 'netcdf':
        file_path = Path(dir_path, str(uuid.uuid1()) + '.nc')
        combined.attrs = metadata_dict
        combined.to_netcdf(file_path)
    delete_file_queue[file_path] = datetime.now()
    logger.debug('Processing request finished {}'.format(errorString))
    return render_template('result.html',
                           download_link='/EnvDataAPI/' + str(file_path.name),
                           download_text='Download requested {} file '.format(data_format),
                           note='The file will be automatically deleted in {} Minutes: {}.'.format(
                               FILE_LIFE_SPAN,
                               (datetime.now(pytz.utc) + timedelta(minutes=FILE_LIFE_SPAN)).strftime("%I:%M %p %Z")
                           ),
                           errorFlag=len(errorString) > 0, error=errorString)


@app.route('/EnvDataAPI/<path:filename>')
def send_file(filename):
    return send_from_directory(directory='download', filename=filename)


@app.route('/EnvDataAPI/', methods=['GET'])
def index():
    return render_template('index.html', max_lat=max_lat, max_lon=max_lon, max_days = max_days)


if __name__ == '__main__':
    serve(TransLogger(app, logger=logger), host='0.0.0.0', port=8080, url_scheme='https')
