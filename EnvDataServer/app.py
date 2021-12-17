import os
import threading
import traceback
from pathlib import Path
import logging
from flask import Flask, render_template, request, send_from_directory, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from utilities.helper_functions import str_to_date_min, create_csv, FileFailedException
from EnvironmentalData.weather import get_global_wave, get_global_wind, get_GFS, get_global_phy_daily, append_to_csv
from datetime import timedelta, datetime
import xarray as xr
import uuid
import numpy as np
import time
import pytz
from waitress import serve
from paste.translogger import TransLogger
import json

logger = logging.getLogger('EnvDataServer.app')

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["70 per hour"]
)

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 Mb limit

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

BASE_URL = os.getenv("BASE_URL", "http://localhost:5000/")


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
    def _filter(values):
        if len(values) == 1 and ',' in values[0]:
            return values[0].split(',')
        else:
            return values
    wave, wind, gfs, phy = [], [], [], []
    logger.debug(type(args))
    if type(args) is dict:
        args_dict = args
    else:
        args_dict = args.to_dict(flat=False)
    if 'Wave' in args_dict.keys():
        wave = _filter(args_dict.get('Wave'))
    if 'Wind' in args_dict.keys():
        wind = _filter(args_dict.get('Wind'))
    if 'GFS' in args_dict.keys():
        gfs = _filter(args_dict.get('GFS'))
    if 'Physical' in args_dict.keys():
        phy = _filter(args_dict.get('Physical'))
    return wave, wind, gfs, phy


@app.route('/EnvDataAPI/merge_data', methods=['POST'])
@limiter.limit("1/10second")
def merge_data():
    logger.debug("Accept header: {}".format(request.accept_mimetypes))
    logger.debug(request)
    error_msg = ''
    wave, wind, gfs, phy = parse_requested_var(json.loads(request.form['var']))
    logger.debug("Requested variables: wind: {}; wave: {}; gfs: {}; physical: {}".format(
        wind, wave, gfs, phy
    ))
    col_dict = json.loads(request.form['col'])
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

    download_link = '{}EnvDataAPI/{}'.format(BASE_URL, str(file_path_up.name))
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


@app.route('/EnvDataAPI/request_env_data', methods=['GET'])
@limiter.limit("1/10second")
def request_env_data():
    logger.debug("Accept header: {}".format(request.accept_mimetypes))
    logger.debug(request)
    dataset_list = []
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
            response.status_code = 400
            return response
        else:
            response = jsonify(error=error)
            response.status_code = 400
            return response
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
    error = []
    if data_format is None or len(data_format) == 0 or data_format.lower() not in ["csv", "netcdf"]:
        error.append('format parameter wrong/missing. Allowed values: csv, netcdf')
    if lat_lo > lat_hi:
        error.append('lat_lo > lat_hi')
    if lon_lo > lon_hi:
        error.append('lon_lo > lon_hi')
    if date_lo > date_hi:
        error.append('date_lo > date_hi')

    wave, wind, gfs, phy = parse_requested_var(request.args)
    logger.debug("Requested variables: wind: {}; wave: {}; gfs: {}; physical: {}".format(
        wind, wave, gfs, phy
    ))
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

    download_link = '{}EnvDataAPI/{}'.format(BASE_URL, str(file_path.name))
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


@app.route('/EnvDataAPI/<path:filename>')
def send_file(filename):
    return send_from_directory(directory='download', filename=filename)


@app.route('/EnvDataAPI/', methods=['GET'])
def index():
    return render_template('index.html', max_lat=max_lat, max_lon=max_lon, max_days=max_days)


if __name__ == '__main__':
    serve(TransLogger(app, logger=logger), port=8080)
