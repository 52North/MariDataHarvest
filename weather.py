import logging
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from bs4 import BeautifulSoup
from motu_utils.utils_cas import authenticate_CAS_for_URL
from motu_utils.utils_http import open_url
from siphon import http_util
from siphon.catalog import TDSCatalog
from xarray.backends import NetCDF4DataStore

from ais import check_dir
from check_connection import CheckConnection
from config import config

# utils to convert dates
str_to_date = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
date_to_str = lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ')

logger = logging.getLogger(__name__)

WIND_VAR_LIST = ['surface_downward_eastward_stress', 'wind_stress_divergence', 'northward_wind', 'sampling_length',
                 'wind_speed_rms', 'wind_vector_curl',
                 'northward_wind_rms', 'eastward_wind', 'wind_speed', 'wind_vector_divergence', 'wind_stress',
                 'wind_stress_curl', 'eastward_wind_rms', 'surface_type',
                 'surface_downward_northward_stress']

WAVE_VAR_LIST = ['VHM0_WW', 'VMDR_SW2', 'VMDR_SW1', 'VMDR', 'VTM10', 'VTPK', 'VPED',
                 'VTM02', 'VMDR_WW', 'VTM01_SW2', 'VHM0_SW1',
                 'VTM01_SW1', 'VSDX', 'VSDY', 'VHM0', 'VTM01_WW', 'VHM0_SW2']

DAILY_PHY_VAR_LIST = ['thetao', 'so', 'uo', 'vo', 'zos', 'mlotst', 'bottomT', 'siconc', 'sithick', 'usi', 'vsi']

GFS_25_VAR_LIST = ['Temperature_surface', 'Wind_speed_gust_surface', 'u-component_of_wind_maximum_wind',
                   'v-component_of_wind_maximum_wind', 'Dewpoint_temperature_height_above_ground',
                   'U-Component_Storm_Motion_height_above_ground_layer',
                   'V-Component_Storm_Motion_height_above_ground_layer', 'Relative_humidity_height_above_ground']

GFS_50_VAR_LIST = variables = ['Temperature_surface', 'u-component_of_wind_maximum_wind',
                               'v-component_of_wind_maximum_wind', 'U-Component_Storm_Motion_height_above_ground_layer',
                               'V-Component_Storm_Motion_height_above_ground_layer',
                               'Relative_humidity_height_above_ground']


def get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points):
    """
        retrieve all wave variables for a specific timestamp, latitude, longitude concidering
        the temporal resolution of the dataset to calculate interpolated values
    """
    logger.debug('obtaining GLOBAL_REANALYSIS_WAV dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    dataset_temporal_resolution = 180
    if date_lo >= datetime(2019, 1, 1, 6):
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSIS_FORECAST_WAV_001_027-TDS'
        product = 'global-analysis-forecast-wav-001-027'
    elif date_lo >= datetime(1993, 1, 1, 6):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_WAV_001_032-TDS'
        product = 'global-reanalysis-wav-001-032'

    y_lo = float(lat_lo)
    y_hi = float(lat_hi)
    x_lo = float(lon_lo)
    x_hi = float(lon_hi)

    # time lower
    time_in_min = (date_lo.hour * 60) + date_lo.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date_lo - timedelta(minutes=rest)

    # time upper
    time_in_min = (date_hi.hour * 60) + date_hi.minute
    rest = time_in_min % dataset_temporal_resolution
    t_hi = date_hi + timedelta(minutes=dataset_temporal_resolution - rest)

    url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
        x_lo, x_hi, y_lo,
        y_hi,
        date_to_str(
            t_lo),
        date_to_str(
            t_hi))

    dataset = try_get_data(url)
    return dataset.interp(longitude=lon_points, latitude=lat_points, time=time_points).to_dataframe()[
        WAVE_VAR_LIST]


def get_global_phy_hourly(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    """
        retrieve <phy> including ... variables for a specific timestamp, latitude, longitude considering
        the temporal resolution of the dataset to calculate interpolated values
    """
    if date_lo < datetime(2019, 1, 1): return None
    logger.debug('obtaining GLOBAL_ANALYSIS_FORECAST_PHY Hourly dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    CheckConnection.set_url('nrt.cmems-du.eu')
    base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload&service=GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS'
    products = ['global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh',
                'global-analysis-forecast-phy-001-024-hourly-merged-uv']
    dataset_temporal_resolution = 60
    time_in_min = (date_lo.hour * 60) + date_lo.minute
    rest = time_in_min % dataset_temporal_resolution

    # available times are at min 30 of each hour
    if date_lo.minute >= 30:
        t_lo = date_lo - timedelta(minutes=rest) + timedelta(minutes=30)
    else:
        t_lo = date_lo - timedelta(minutes=rest) - timedelta(minutes=30)

    time_in_min = (date_hi.hour * 60) + date_hi.minute
    rest = time_in_min % dataset_temporal_resolution

    if date_hi.minute >= 30:
        t_hi = date_hi + timedelta(minutes=(dataset_temporal_resolution - rest)) + timedelta(minutes=30)
    else:
        t_hi = date_hi + timedelta(minutes=(dataset_temporal_resolution - rest)) - timedelta(minutes=30)

    # coordinates
    y_lo = float(lat_lo)
    y_hi = float(lat_hi)
    x_lo = float(lon_lo)
    x_hi = float(lon_hi)

    # depth
    z_hi = 0.50
    z_lo = 0.49

    url = base_url + '&product=' + products[0] + '&product=global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh' + \
          '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                         y_lo,
                                                                                                         y_hi,
                                                                                                         date_to_str(
                                                                                                             t_lo)
                                                                                                         , date_to_str(
                  t_hi), z_lo, z_hi)
    data = try_get_data(url)
    return data


def try_get_data(url):
    try:
        CheckConnection.is_online()
        url_auth = authenticate_CAS_for_URL(url, config['UN_CMEMS'], config['PW_CMEMS'])
        response = open_url(url_auth)
        CheckConnection.is_online()
        read_bytes = response.read()
        CheckConnection.is_online()
        return xr.open_dataset(read_bytes)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise ValueError('Error:', BeautifulSoup(read_bytes, 'html.parser').find('p', {"class": "error"}), 'Request: ',
                         url, response)


def get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points):
    logger.debug('obtaining WIND_GLO_WIND_L4_NRT_OBSERVATIONS dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    dataset_temporal_resolution = 360
    if date_lo >= datetime(2018, 1, 1, 6):
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004-TDS'
        product = 'CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE'
    elif date_lo >= datetime(1992, 1, 1, 6):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_WIND_L4_REP_OBSERVATIONS_012_006-TDS'
        product = 'CERSAT-GLO-BLENDED_WIND_L4_REP-V6-OBS_FULL_TIME_SERIE'

    time_in_min = (date_lo.hour * 60) + date_lo.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date_lo - timedelta(minutes=rest)  # extract the lower bound

    time_in_min = (date_hi.hour * 60) + date_hi.minute
    rest = time_in_min % dataset_temporal_resolution
    t_hi = date_hi + timedelta(minutes=dataset_temporal_resolution - rest)

    y_lo = float(lat_lo)
    y_hi = float(lat_hi)
    x_lo = float(lon_lo)
    x_hi = float(lon_hi)

    url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
        x_lo, x_hi, y_lo,
        y_hi,
        date_to_str(
            t_lo),
        date_to_str(
            t_hi))
    dataset = try_get_data(url)
    return dataset.interp(lon=lon_points, lat=lat_points, time=time_points).to_dataframe()[WIND_VAR_LIST]


def get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points):
    logger.debug('obtaining GFS 0.25 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    start_date = datetime(date_lo.year, date_lo.month, date_lo.day) - timedelta(days=1)
    # consider the supported time range
    if start_date < datetime(2015, 1, 15):
        logger.debug('GFS 0.25 DATASET is out of supported range')
        return get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points)
    x_arr_list = []
    base_url = 'https://rda.ucar.edu/thredds/catalog/files/g/ds084.1'
    CheckConnection.set_url('rda.ucar.edu')
    # calculate a day prior for midnight interpolation
    http_util.session_manager.set_session_options(auth=(config['UN_RDA'], config['PW_RDA']))
    start_cat = TDSCatalog(
        "%s/%s/%s%.2d%.2d/catalog.xml" % (base_url, start_date.year, start_date.year, start_date.month, start_date.day))
    ds_subset = start_cat.datasets[
        'gfs.0p25.%s%.2d%.2d18.f006.grib2' % (start_date.year, start_date.month, start_date.day)].subset()
    query = ds_subset.query().lonlat_box(north=lat_hi, south=lat_lo, east=lon_hi, west=lon_lo).variables(
        *GFS_25_VAR_LIST)
    CheckConnection.is_online()
    data = ds_subset.get_data(query)
    x_arr = xr.open_dataset(NetCDF4DataStore(data))
    if 'time1' in list(x_arr.coords):
        x_arr = x_arr.rename({'time1': 'time'})
    x_arr_list.append(x_arr)

    for day in range((date_hi - date_lo).days + 1):
        end_date = datetime(date_lo.year, date_lo.month, date_lo.day) + timedelta(days=day)
        end_cat = TDSCatalog(
            "%s/%s/%s%.2d%.2d/catalog.xml" % (base_url, end_date.year, end_date.year, end_date.month, end_date.day))
        for cycle in [0, 6, 12, 18]:
            for hours in [3, 6]:
                name = 'gfs.0p25.%s%.2d%.2d%.2d.f0%.2d.grib2' % (
                    end_date.year, end_date.month, end_date.day, cycle, hours)
                if name in list(end_cat.datasets):
                    ds_subset = end_cat.datasets[name].subset()
                    query = ds_subset.query().lonlat_box(north=lat_hi, south=lat_lo, east=lon_hi,
                                                         west=lon_lo).variables(*GFS_25_VAR_LIST)
                    CheckConnection.is_online()
                    data = ds_subset.get_data(query)
                    x_arr = xr.open_dataset(NetCDF4DataStore(data))
                    if 'time1' in list(x_arr.coords):
                        x_arr = x_arr.rename({'time1': 'time'})
                    x_arr_list.append(x_arr)
                else:
                    logger.warning('dataset %s is not found' % name)
    dataset = xr.combine_by_coords(x_arr_list).squeeze()
    lon_points = ((lon_points + 180) % 360) + 180
    b = xr.DataArray([1] * len(lon_points))
    res = dataset.interp(longitude=lon_points, latitude=lat_points, time=time_points, bounds_dim=b).to_dataframe()[
        GFS_25_VAR_LIST]
    return res


def get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points):
    logger.debug('obtaining GFS 0.50 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    base_url = 'https://www.ncei.noaa.gov/thredds/model-gfs-g4-anl-files-old/'
    CheckConnection.set_url('ncei.noaa.gov')

    x_arr_list = []
    start_date = datetime(date_lo.year, date_lo.month, date_lo.day) - timedelta(days=1)
    for day in range((date_hi - start_date).days + 1):
        dt = datetime(start_date.year, start_date.month, start_date.day) + timedelta(days=day)
        catalog = TDSCatalog(
            '%s%s%.2d/%s%.2d%.2d/catalog.xml' % (base_url, dt.year, dt.month, dt.year, dt.month, dt.day))
        for hour in [3, 6]:
            for cycle in [0, 6, 12, 18]:
                attempts = 0
                while True:
                    try:
                        attempts += 1
                        name = 'gfsanl_4_%s%.2d%.2d_%.2d00_00%s.grb2' % (dt.year, dt.month, dt.day, cycle, hour)
                        if name in list(catalog.datasets):
                            ds_subset = catalog.datasets[name].subset()
                            query = ds_subset.query().lonlat_box(north=lat_hi, south=lat_lo, east=lon_hi,
                                                                 west=lon_lo).variables(*GFS_50_VAR_LIST)
                            CheckConnection.is_online()
                            data = ds_subset.get_data(query)
                            x_arr = xr.open_dataset(NetCDF4DataStore(data))
                            if 'time1' in list(x_arr.coords):
                                x_arr = x_arr.rename({'time1': 'time'})
                            x_arr_list.append(x_arr)
                        else:
                            logger.warning('dataset %s is not found' % name)
                        break
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        CheckConnection.is_online()
                        logger.error(e)
                        logger.error('Filename %s - Failed connecting to GFS Server - number of attempts: %d' % (
                            name, attempts))
                        time.sleep(2)

    dataset = xr.combine_by_coords(x_arr_list).squeeze()
    lon_points = ((lon_points + 180) % 360) + 180
    res = dataset.interp(lon=lon_points, lat=lat_points, time=time_points).to_dataframe()[GFS_50_VAR_LIST]
    res[['Wind_speed_gust_surface', 'Dewpoint_temperature_height_above_ground']] = [[np.nan, np.nan]] * len(res)
    return res


def get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points, lon_points):
    logger.debug('obtaining GLOBAL_ANALYSIS_FORECAST_PHY Daily dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    if date_lo >= datetime(2019, 1, 2):
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS'
        product = 'global-analysis-forecast-phy-001-024'
    elif date_lo >= datetime(1993, 1, 2):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_PHY_001_030-TDS'
        product = 'global-reanalysis-phy-001-030-daily'

    t_lo = datetime(date_lo.year, date_lo.month, date_lo.day, 12) - timedelta(days=1)
    t_hi = datetime(date_hi.year, date_hi.month, date_hi.day, 12) + timedelta(days=1)

    # coordinates
    y_lo = float(lat_lo)
    y_hi = float(lat_hi)
    x_lo = float(lon_lo)
    x_hi = float(lon_hi)

    # depth
    z_hi = 0.50
    z_lo = 0.49

    url = base_url + '&service=' + service + '&product=' + product + \
          '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                         y_lo,
                                                                                                         y_hi,
                                                                                                         date_to_str(
                                                                                                             t_lo)
                                                                                                         , date_to_str(
                  t_hi), z_lo, z_hi)
    dataset = try_get_data(url)
    return dataset.interp(longitude=lon_points, latitude=lat_points, time=time_points).to_dataframe()[
        DAILY_PHY_VAR_LIST].reset_index(drop=True, inplace=True)


def append_to_csv(in_path: Path, out_path: Path) -> None:
    logger.debug('append_environment_data in file %s' % in_path)
    chunkSize = 100000
    header = True
    try:
        for df_chunk in pd.read_csv(in_path, parse_dates=['BaseDateTime'], date_parser=str_to_date,
                                    chunksize=chunkSize):
            if len(df_chunk) > 1:
                # remove index column
                df_chunk.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)

                # retrieve the data for each file once
                lat_hi = df_chunk.LAT.max()
                lon_hi = df_chunk.LON.max()

                lat_lo = df_chunk.LAT.min()
                lon_lo = df_chunk.LON.min()

                date_lo = df_chunk.BaseDateTime.min()
                date_hi = df_chunk.BaseDateTime.max()
                time_points = xr.DataArray(list(df_chunk['BaseDateTime'].values))
                lat_points = xr.DataArray(list(df_chunk['LAT'].values))
                lon_points = xr.DataArray(list(df_chunk['LON'].values))

                df_chunk = pd.concat([df_chunk, get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points,
                                                        lat_points, lon_points)], axis=1)

                df_chunk = pd.concat(
                    [df_chunk, get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points,
                                                    lat_points, lon_points)], axis=1)

                df_chunk = pd.concat(
                    [df_chunk, get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points,
                                               lon_points)], axis=1)

                df_chunk = pd.concat(
                    [df_chunk, get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi, time_points, lat_points,
                                               lon_points)], axis=1)

                df_chunk.to_csv(out_path, chunksize=chunkSize, mode='a', header=header)
                header = False
    except Exception as e:
        # discard the file in case of an error to resume later properly
        if out_path:
            out_path.unlink(missing_ok=True)
        raise e


def append_environment_data_to_year(filtered_dir: Path, merged_dir: Path) -> None:
    csv_list = check_dir(filtered_dir)
    for file in csv_list:
        if Path(merged_dir, file).exists(): continue
        append_to_csv(Path(filtered_dir, file), Path(merged_dir, file))


def append_environment_data_to_file(file_name, filtered_dir, merged_dir):
    append_to_csv(Path(filtered_dir, file_name), Path(merged_dir, file_name))
