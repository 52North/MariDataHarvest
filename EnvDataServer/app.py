#   Copyright (C) 2021 - 2023 52°North Spatial Information Research GmbH
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# If the program is linked with libraries which are licensed under one of
# the following licenses, the combination of the program with the linked
# library is not considered a "derivative work" of the program:
#
#     - Apache License, version 2.0
#     - Apache Software License, version 1.0
#     - GNU Lesser General Public License, version 3
#     - Mozilla Public License, versions 1.0, 1.1 and 2.0
#     - Common Development and Distribution License (CDDL), version 1.0
#
# Therefore the distribution of the program linked with libraries licensed
# under the aforementioned licenses, is permitted by the copyright holders
# if the distribution is compliant with both the GNU General Public
# License version 2 and the aforementioned licenses.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
import json
import logging
import os
import threading
import time
import traceback
import uuid
from datetime import timedelta, datetime
from pathlib import Path

import numpy as np
import pytz
import xarray as xr
from flask import Flask, render_template, request, send_from_directory, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from paste.translogger import TransLogger
from waitress import serve

from EnvironmentalData.weather import *
from utilities.helper_functions import str_to_date_min, create_csv, FileFailedException

logger = logging.getLogger('EnvDataServer.app')

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["70 per hour"]
)
app.config.from_object('EnvDataServer.config')

# global variables
# TODO move to a config file
# Use https://flask.palletsprojects.com/en/1.1.x/config/#configuration-basics
delete_file_queue = dict()
# in Minutes
FILE_LIFE_SPAN = int(os.getenv("FILE_LIFE_SPAN", 120))
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
    logger.debug(type(args))

    if type(args) is dict:
        args_dict = args
    else:
        args_dict = args.to_dict(flat=False)

    def _filter(values):
        if len(values) == 1 and ',' in values[0]:
            return values[0].split(',')
        else:
            return values

    wave, wind, gfs, phy = [], [], [], []
    if 'Wave' in args_dict.keys():
        wave = _filter(args_dict.get('Wave'))
    if 'Wind' in args_dict.keys():
        wind = _filter(args_dict.get('Wind'))
    if 'GFS' in args_dict.keys():
        gfs = _filter(args_dict.get('GFS'))
    if 'Physical' in args_dict.keys():
        phy = _filter(args_dict.get('Physical'))

    # todo: check each array for wrong entries
    def _check_for_unknown_values(values, allowed_values, unknown_values):
        for var in values:
            if var not in allowed_values:
                unknown_values.append(var)
        return unknown_values

    unknown_values = []
    _check_for_unknown_values(wave, WAVE_VAR_LIST, unknown_values)
    _check_for_unknown_values(wind, WIND_VAR_LIST, unknown_values)
    _check_for_unknown_values(gfs, GFS_25_VAR_LIST, unknown_values)
    _check_for_unknown_values(phy, DAILY_PHY_VAR_LIST, unknown_values)

    return wave, wind, gfs, phy, unknown_values


@app.route('/merge_data', methods=['POST'])
@limiter.limit("1/10second")
def merge_data():
    logger.debug("Accept header: {}".format(request.accept_mimetypes))
    logger.debug(request)
    error_msg = ''

    error = []
    if 'col' not in request.form.keys():
        error.append('col')
    if 'var' not in request.form.keys():
        error.append('var')
    if len(request.files) == 0 or 'file' not in request.files.keys():
        error.append('file')

    if len(error) > 0:
        error = 'Missing mandatory parameter{}: {}'.format('s' if len(error) > 1 else '', error)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            response = render_template('error.html', error=error)
            return response, 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    col_dict = json.loads(request.form['col'])
    unknown_cols = []
    for key in col_dict.keys():
        if key not in ['time', 'lat', 'lon']:
            unknown_cols.append(key)

    if len(unknown_cols) > 0:
        error = 'Received unknown column mapping{}: {}'.format('s' if len(unknown_cols) > 1 else '', unknown_cols)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            response = render_template('error.html', error=error)
            return response, 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    selected_variables = json.loads(request.form['var'])
    unknown_variables = []
    for key in selected_variables.keys():
        # TODO make list of supported variables configurable to be able to disable them
        if key not in ["GFS", "Physical", "Wave", "Wind"]:
            unknown_variables.append(key)

    if len(unknown_variables) > 0:
        error = 'Received unknown parameter{}: {}'.format('s' if len(unknown_variables) > 1 else '', unknown_variables)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            response = render_template('error.html', error=error)
            return response, 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    wave, wind, gfs, phy, unknown_values = parse_requested_var(selected_variables)
    logger.debug("Requested variables: wind: {}; wave: {}; gfs: {}; physical: {}; unknown: {}".format(
        wind, wave, gfs, phy, unknown_values
    ))
    if len(unknown_values) > 0:
        error = 'Error: unknown variables submitted: {}'.format(unknown_values)
        logger.error(error)
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response
    if len(wave + wind + gfs + phy) == 0:
        error = 'Error: No variables are selected'
        logger.error(error)
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response
    file = request.files['file']
    dir_path_up = Path(Path(__file__).parent, 'upload')
    dir_path_up.mkdir(exist_ok=True)
    dir_path_down = Path(Path(__file__).parent, 'download')
    dir_path_down.mkdir(exist_ok=True)
    filename = str(uuid.uuid1()) + '.csv'
    file_path_up = Path(dir_path_up, filename)
    file_path_down = Path(dir_path_down, filename)
    file.save(file_path_up)
    try:
        metadata_dict = dict(
            credit_CMEMS='Credit (Wave-Wind-Physical): E.U. Copernicus Marine Service Information (CMEMS)',
            credit_GFS='Credit (GFS): National Centers for Environmental Prediction/National Weather Service/NOAA',
            created='Accessed on %s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            errors='Error(s): ' + error_msg.replace(',', ' ').replace('\n', ' ')
        )
        append_to_csv(file_path_up, file_path_down, wave=wave, wind=wind, gfs=gfs, phy=phy, col_dict=col_dict,
                      metadata=metadata_dict, webapp=True)
    except FileFailedException as e:
        logger.error(traceback.format_exc())
        error_msg = 'CSV file is not valid: Error occurred while appending env data: \"' + str(
            e.original_exception) + '\"'
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error_msg), 400
        else:
            response = jsonify(error=error_msg)
            response.status_code = 400
            return response
    # TODO should we remove uploaded data?
    delete_file_queue[str(file_path_up)] = datetime.now() + timedelta(minutes=FILE_LIFE_SPAN)
    delete_file_queue[str(file_path_down)] = datetime.now()

    download_link = '{}EnvDataAPI/{}'.format(app.config['BASE_URL'], str(file_path_up.name))
    file_end_of_life = (datetime.now(pytz.utc) + timedelta(minutes=FILE_LIFE_SPAN))

    if request.accept_mimetypes['text/html']:
        download_text = 'Download merged csv file'
        note = 'The file will be automatically deleted in {} Minutes: {}.'.format(FILE_LIFE_SPAN,
                                                                                  file_end_of_life.strftime(
                                                                                      "%I:%M %p %Z"))
        return render_template('result.html',
                               download_link=download_link,
                               download_text=download_text,
                               note=note,
                               errorFlag=len(error_msg) > 0,
                               error=error_msg)
    else:
        json_response = {
            'link': download_link,
            'limit': file_end_of_life.isoformat(timespec='seconds')
        }
        if len(error_msg) > 0:
            json_response.update({
                'error': error_msg
            })
        return jsonify(json_response)

@app.route('/request_env_data', methods=['GET'])
@limiter.limit("1/10second")
def request_env_data():
    logger.debug("Accept header: {}".format(request.accept_mimetypes))
    logger.debug(request)
    error = []
    # check for mandatory parameter
    if 'date_lo' not in request.args:
        error.append('date_lo')
    if 'date_hi' not in request.args:
        error.append('date_hi')
    if 'lat_lo' not in request.args:
        error.append('lat_lo')
    if 'lat_hi' not in request.args:
        error.append('lat_hi')
    if 'lon_lo' not in request.args:
        error.append('lon_lo')
    if 'lon_hi' not in request.args:
        error.append('lon_hi')

    if len(error) > 0:
        error = 'Missing mandatory parameter{}: {}'.format('s' if len(error) > 1 else '', error)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            response = render_template('error.html', error=error)
            return response, 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    unknown_parameter = []
    for key in request.args.keys():
        if key not in ["date_lo", "date_hi" ,"lat_lo", "lat_hi", "lon_lo", "lon_hi", "format", "GFS", "Physical", "Wave", "Wind"]:
            unknown_parameter.append(key)

    if len(unknown_parameter) > 0:
        error = 'Received unknown parameter{}: {}'.format('s' if len(unknown_parameter) > 1 else '', unknown_parameter)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            response = render_template('error.html', error=error)
            return response, 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    try:
        date_lo = str_to_date_min(request.args.get('date_lo')) - timedelta(hours=temporal_interpolation_rate)
        date_hi = str_to_date_min(request.args.get('date_hi')) + timedelta(hours=temporal_interpolation_rate)
        lat_lo = float(request.args.get('lat_lo')) - spatial_interpolation_rate
        lat_hi = float(request.args.get('lat_hi')) + spatial_interpolation_rate
        lon_lo = float(request.args.get('lon_lo')) - spatial_interpolation_rate
        lon_hi = float(request.args.get('lon_hi')) + spatial_interpolation_rate
    except Exception as e:
        logger.error(traceback.format_exc())
        error = 'Error occurred: not all submitted parameters could not be parsed as date: {}, {}, or float: {}, {}, {}, {}. {}'.format(
            request.args.get('date_lo'),
            request.args.get('date_hi'),
            request.args.get('lat_lo'),
            request.args.get('lat_hi'),
            request.args.get('lon_lo'),
            request.args.get('lon_hi'),
            str(e)
        )
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    #
    #   rounding coordinates to reasonable accuracy
    #
    #   see https://gis.stackexchange.com/a/208739
    #
    lat_lo = float(lat_lo) 
    lat_lo = float("%.4f" % lat_lo)
    lat_hi = float(lat_hi)
    lat_hi = float("%.4f" % lat_hi)
    lon_hi = float(lon_hi)
    lon_hi = float("%.4f" % lon_hi)
    lon_lo = float(lon_lo)
    lon_lo = float("%.4f" % lon_lo)
    logger.debug("Rounded coordinates to 4 decimal places: Lat: [{}, {}]; Lon: [{}, {}]".format(
        lat_lo, lat_hi, lon_lo, lon_hi
    ))
    data_format = request.args.get('format')
    error = []
    if data_format is None or len(data_format) == 0 or data_format.lower() not in ["csv", "netcdf"]:
        error.append('format parameter wrong/missing. Allowed values: csv, netcdf')
    if lat_lo > lat_hi:
        error.append('lat_lo > lat_hi')
    if lon_lo > lon_hi:
        error.append('lon_lo > lon_hi')
    if date_lo > date_hi:
        error.append('date_lo > date_hi')

    wave, wind, gfs, phy, unknown_values = parse_requested_var(request.args)

    logger.debug("Requested variables: wind: {}; wave: {}; gfs: {}; physical: {}; unknown: {}".format(
        wind, wave, gfs, phy, unknown_values
    ))

    if len(unknown_values) > 0:
        error = 'Error: unknown variables submitted: {}'.format(unknown_values)
        logger.error(error)
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    if len(wave + wind + gfs + phy) == 0:
        error.append('No variables are selected')

    if len(error) > 0:
        logger.debug('Error{}: {}'.format('s' if len(error) > 1 else '', error))
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    if int(lat_hi - lat_lo) > max_lat or int(lon_hi - lon_lo) > max_lon or (date_hi - date_lo).days > max_days:
        error = 'Error occurred: requested bbox ({0}° lat x {1}° lon x {2} days) is too large. Maximal bbox ' + \
                'dimension ({3}° lat x {4}° lon x {5} days).'.format(
                    int(lat_hi - lat_lo),
                    int(lon_hi - lon_lo),
                    (date_hi - date_lo).days,
                    max_lat,
                    max_lon,
                    max_days)
        logger.debug(error)
        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), 400
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response

    error_msg = ''

    lat_interpolation = list(np.arange(lat_lo, lat_hi, spatial_interpolation_rate))
    lon_interpolation = list(np.arange(lon_lo, lon_hi, spatial_interpolation_rate))
    temporal_interpolation = [date_lo + timedelta(hours=hours) for hours in
                              range(0, (date_hi - date_lo).days * 24 + (date_hi - date_lo).seconds // 3600,
                                    temporal_interpolation_rate)]

    def rescale_dataset(dataset: xr.Dataset) -> xr.Dataset:
        return dataset.interp(
            latitude=xr.DataArray(lat_interpolation, coords=[lat_interpolation], dims=["latitude"]),
            longitude=xr.DataArray(lon_interpolation, coords=[lon_interpolation], dims=["longitude"]),
            time=xr.DataArray(temporal_interpolation, coords=[temporal_interpolation], dims=["time"]))

    dataset_list = []

    if len(wave) > 0:
        try:
            with get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0] as wave_ds:
                dataset_list.append(rescale_dataset(wave_ds))
                wave = [var for var in wave if var in list(wave_ds.keys())]
        except Exception as e:
            logger.error(traceback.format_exc())
            wave = []
            error_msg += 'Error occurred while retrieving Wave data: ' + str(e) + '\n'

    if len(wind) > 0:
        try:
            with get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].rename(
                    {'lat': 'latitude', 'lon': 'longitude'}) as dataset_wind:
                dataset_list.append(rescale_dataset(dataset_wind))
                wind = [var for var in wind if var in list(dataset_wind.keys())]
        except Exception as e:
            logger.error(traceback.format_exc())
            wind = []
            error_msg += 'Error occurred while retrieving Wind data: ' + str(e) + '\n'

    if len(phy) > 0:
        try:
            with get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)[0].squeeze() as dataset_phy:
                dataset_list.append(rescale_dataset(dataset_phy))
                phy = [var for var in phy if var in list(dataset_phy.keys())]
        except Exception as e:
            logger.error(traceback.format_exc())
            phy = []
            error_msg += 'Error occurred while retrieving Physical data:  ' + str(e) + '\n'

    if len(gfs) > 0:
        try:
            dataset_gfs, gfs_type = get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
            if gfs_type == 'gfs_50':
                dataset_gfs = dataset_gfs.rename({'lat': 'latitude', 'lon': 'longitude'})
            dataset_list.append(rescale_dataset(dataset_gfs))
            gfs = [var for var in gfs if var in list(dataset_gfs.keys())]
        except Exception as e:
            logger.error(traceback.format_exc())
            gfs = []
            error_msg += 'Error occurred while retrieving GFS data: ' + str(e) + '\n'

    combined = xr.combine_by_coords(dataset_list, combine_attrs='drop', compat='override')[wave + wind + phy + gfs]

    if len(combined) == 0:
        error = 'Empty dataset'
        logger.debug(error_msg + 'Error occurred: {}'.format(error))
        error = error_msg + '\nError occurred: Empty dataset'

        if len(error_msg) == 0:
            status_code = 400
        else:
            status_code = 500

        if request.accept_mimetypes['text/html']:
            return render_template('error.html', error=error), status_code
        else:
            response = jsonify(error=error)
            response.status_code = status_code
            return response

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
        errors='Error(s): ' + error_msg.replace(',', ' ').replace('\n', ' ')
    )
    if data_format == 'csv':
        file_path = Path(dir_path, str(uuid.uuid1()) + '.csv')
        create_csv(combined.to_dataframe(), metadata_dict, file_path)
    elif data_format == 'netcdf':
        file_path = Path(dir_path, str(uuid.uuid1()) + '.nc')
        combined.attrs = metadata_dict
        combined.to_netcdf(file_path)
    delete_file_queue[file_path] = datetime.now()
    logger.debug('Processing request finished {}'.format(error_msg))

    download_link = '{}{}'.format(app.config['BASE_URL'], str(file_path.name))
    file_end_of_life = (datetime.now(pytz.utc) + timedelta(minutes=FILE_LIFE_SPAN))

    if request.accept_mimetypes['text/html']:
        download_text = 'Download requested {} file '.format(data_format)
        note = 'The file will be automatically deleted in {} Minutes: {}.'.format(FILE_LIFE_SPAN, file_end_of_life.strftime("%I:%M %p %Z"))
        return render_template('result.html',
                               download_link=download_link,
                               download_text=download_text,
                               note=note,
                               errorFlag=len(error_msg) > 0,
                               error=error_msg)
    else:
        json_response = {
            'link': download_link,
            'limit': file_end_of_life.isoformat(timespec='seconds')
        }
        if len(error_msg) > 0:
            json_response.update({
                'error': error_msg
            })
        return jsonify(json_response)


@app.route('/<path:filename>')
def send_file(filename):
    return send_from_directory(directory='download', filename=filename)


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', max_lat=max_lat, max_lon=max_lon, max_days=max_days)


if __name__ == '__main__':
    serve(TransLogger(app, logger=logger), port=8080, url_prefix=app.config['URL_PREFIX'])
