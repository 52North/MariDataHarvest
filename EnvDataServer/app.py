import os
import sys
from pathlib import Path

sys.path.append(os.getcwd())
import logging
from flask import Flask, render_template, request, send_from_directory, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from utilities.helper_functions import str_to_date_min
from EnvironmentalData.weather import get_global_wave, get_global_wind, get_GFS, get_global_phy_daily
from datetime import timedelta
import xarray as xr
import uuid
import numpy as np
import time

app = Flask(__name__, static_folder=os.path.abspath("static/"))
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["70 per hour"]
)
logger = logging.getLogger(__name__)


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


@app.route('/download/request_env_data')
@limiter.limit("1/10second")
def download():
    logger.info(request)
    dataset_list = []
    wave, wind, gfs, phy = parse_requested_var(request.args)
    date_lo = str_to_date_min(request.args.get('date_lo')) - timedelta(hours=1)
    date_hi = str_to_date_min(request.args.get('date_hi')) + timedelta(hours=1)
    lat_lo = float(request.args.get('lat_lo')) - 0.0833
    lat_hi = float(request.args.get('lat_hi')) + 0.0833
    lon_lo = float(request.args.get('lon_lo')) - 0.0833
    lon_hi = float(request.args.get('lon_hi')) + 0.0833
    if lat_lo > lat_hi:
        return Response('Error: lat_lo > lat_hi')
    if lon_lo > lon_hi:
        return Response('Error: lon_lo > lon_hi ')
    if date_lo > date_hi:
        return Response('Error: date_lo > date_hi')
    if len(wave + wind + gfs + phy) == 0:
        return Response('Error: No variables are selected')
    errorString = ''

    lat_interpolation = list(np.arange(lat_lo, lat_hi, 0.083))
    lon_interpolation = list(np.arange(lon_lo, lon_hi, 0.083))
    temporal_interpolation = [date_lo + timedelta(hours=hours) for hours in
                              range(0, (date_hi - date_lo).days * 24 + (date_hi - date_lo).seconds // 3600, 3)]

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

    if int(lat_hi - lat_lo) > 40 and int(lon_hi - lon_lo) > 40 and (date_hi - date_lo).days > 30:
        return Response(
            'Error occurred: requested bbox ({0}° lat x {1}° lon x {2} days) is too large.'.format(int(lat_hi - lat_lo),
                                                                                                   int(lon_hi - lon_lo),
                                                                                                   (
                                                                                                           date_hi - date_lo).days))
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
    credit_cmems = 'Credit (Wave, Wind and Physical): E.U. Copernicus Marine Service Information (CMEMS)' + header
    credit_GFS = 'Credit (GFS): National Centers for Environmental Prediction/National Weather Service/NOAA' + header
    created = 'Accessed on %s' % time.strftime('%Y-%m-%d %H:%M:%S') + header
    csv_str = timeRange + lon + lat + spatial_res + temporal_res + credit_cmems + credit_GFS + created + csv_str
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        f.write(csv_str)
    return errorString + 'Download requested CSV file: <a href="/download/' + str(
        file_path.name) + '"> /download/' + str(
        file_path.name) + '</a>'


@app.route('/download/<path:filename>')
def send_file(filename):
    return send_from_directory(directory='download', filename=filename)


@app.route('/download/', methods=['GET'])
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run()
