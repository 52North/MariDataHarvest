import os
import sys
import threading
from pathlib import Path

sys.path.append(os.getcwd())
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
from waitress import serve

logger = logging.getLogger('EnvDataServer.app')

app = Flask(__name__, static_folder=os.path.abspath("static/"))
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["70 per hour"]
)

# global variables TODO move to a config file
delete_file_queue = dict()
FILE_LIFE_SPAN = 20 * 60  # seconds
spatial_interpolation_rate = 0.083
temporal_interpolation_rate = 3  # hours


def remove_files():
    while True:
        _deep_copy = delete_file_queue.copy()
        for f_path, created_time in _deep_copy.items():
            if (datetime.now() - created_time).seconds > FILE_LIFE_SPAN:
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
        if str(var).startswith('var'):
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


@app.route('/EnvDataAPI/request_env_data')
@limiter.limit("1/10second")
def request_env_data():
    logger.debug(request)
    dataset_list = []
    wave, wind, gfs, phy = parse_requested_var(request.args)
    date_lo = str_to_date_min(request.args.get('date_lo')) - timedelta(hours=1)
    date_hi = str_to_date_min(request.args.get('date_hi')) + timedelta(hours=1)
    lat_lo = float(request.args.get('lat_lo')) - spatial_interpolation_rate
    lat_hi = float(request.args.get('lat_hi')) + spatial_interpolation_rate
    lon_lo = float(request.args.get('lon_lo')) - spatial_interpolation_rate
    lon_hi = float(request.args.get('lon_hi')) + spatial_interpolation_rate
    if lat_lo > lat_hi:
        logger.debug('Error: lat_lo > lat_hi')
        return Response('Error: lat_lo > lat_hi')
    if lon_lo > lon_hi:
        logger.debug('Error: lon_lo > lon_hi ')
        return Response('Error: lon_lo > lon_hi')
    if date_lo > date_hi:
        logger.debug('Error: date_lo > date_hi')
        return Response('Error: date_lo > date_hi')
    if len(wave + wind + gfs + phy) == 0:
        logger.debug('Error: No variables are selected')
        return Response('Error: No variables are selected')
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

    if int(lat_hi - lat_lo) > 20 or int(lon_hi - lon_lo) > 20 or (date_hi - date_lo).days > 10:
        error = 'Error occurred: requested bbox ({0}° lat x {1}° lon x {2} days) is too large. Maximal bbox dimension ({3}° lat x {4}° lon x {5} days).'.format(
            int(lat_hi - lat_lo),
            int(lon_hi - lon_lo), (date_hi - date_lo).days, 20, 20, 10)
        logger.debug(error)
        return Response(error)

    try:
        if len(wave) > 0:
            with get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0] as wave_ds:
                dataset_list.append(rescale_dataset(wave_ds))
                wave = [var for var in wave if var in list(wave_ds.keys())]
    except Exception as e:
        wave = []
        errorString += 'Error occurred while retrieving Wave data:  ' + str(e) + '<br>'

    try:
        if len(wind) > 0:
            with get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].rename(
                    {'lat': 'latitude', 'lon': 'longitude'}) as dataset_wind:
                dataset_list.append(rescale_dataset(dataset_wind))
                wind = [var for var in wind if var in list(dataset_wind.keys())]
    except Exception as e:
        wind = []
        errorString += 'Error occurred while retrieving Wind data:  ' + str(e) + '<br>'

    if len(phy) > 0:
        try:
            with get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].squeeze() as dataset_phy:
                dataset_list.append(rescale_dataset(dataset_phy))
                phy = [var for var in phy if var in list(dataset_phy.keys())]
        except Exception as e:
            phy = []
            errorString += 'Error occurred while retrieving Physical data:  ' + str(e) + '<br>'

    if len(gfs) > 0:
        try:
            with get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].rename(
                    {'lat': 'latitude', 'lon': 'longitude'}) as dataset_gfs:
                dataset_list.append(rescale_dataset(dataset_gfs, gfs_flag=True))
                gfs = [var for var in gfs if var in list(dataset_gfs.keys())]
        except Exception as e:
            gfs = []
            errorString += 'Error occurred while retrieving GFS data:  ' + str(e) + '<br>'

    combined = xr.combine_by_coords(dataset_list, combine_attrs='override', compat='override')
    if len(combined) == 0:
        logger.debug(errorString + 'Error occurred: Empty dataset')
        return Response(errorString + 'Error occurred: Empty dataset')
    dir_path = Path(Path(__file__).parent, 'download')
    dir_path.mkdir(exist_ok=True)
    file_path = Path(dir_path, str(uuid.uuid1()) + '.csv')
    csv_str = combined.to_dataframe()[wave + wind + phy + gfs].to_csv()
    header = csv_str[:csv_str.find('\n')].count(',') * ',' + '\n'
    timeRange = 'Time range: %s to %s' % (str(date_lo), str(date_hi)) + header
    lon = 'Longitude extent: %.2f to %.2f' % (lon_lo, lon_hi) + header
    lat = 'Latitude extent: %.2f to %.2f' % (lat_lo, lat_hi) + header
    spatial_res = 'Spatial Resolution 0.083deg x 0.083deg' + header
    temporal_res = 'Temporal Resolution 3-hours interval' + header
    credit_CMEMS = 'Credit (Wave-Wind-Physical): E.U. Copernicus Marine Service Information (CMEMS)' + header
    credit_GFS = 'Credit (GFS): National Centers for Environmental Prediction/National Weather Service/NOAA' + header
    created = 'Accessed on %s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S') + header
    err = ''
    if len(errorString) > 0:
        err = 'Error: ' + errorString + header
    csv_str = timeRange + lon + lat + spatial_res + temporal_res + credit_CMEMS + credit_GFS + created + err + csv_str
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        f.write(csv_str)
    resp = errorString + '<a href="/EnvDataAPI/' + str(
        file_path.name) + '"> Download requested CSV file</a> <strong> The file will be deleted after 20 Minutes automatically.</strong>'
    delete_file_queue[file_path] = datetime.now()
    logger.debug(resp)
    return resp


@app.route('/EnvDataAPI/<path:filename>')
def send_file(filename):
    return send_from_directory(directory='download', filename=filename)


@app.route('/EnvDataAPI/', methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == '__main__':
    serve(app, host="localhost", port=80)