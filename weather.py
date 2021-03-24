import logging
import pandas as pd
import numpy as np
from motu_utils.utils_cas import authenticate_CAS_for_URL
from motu_utils.utils_http import open_url
import xarray as xr
from datetime import datetime, timedelta
import time
from pathlib import Path
from bs4 import BeautifulSoup
from ais import check_dir
from xarray.backends import NetCDF4DataStore
from siphon.catalog import TDSCatalog
from siphon import http_util
import os
from check_connection import CheckConnection

# utils to convert dates
str_to_date = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
date_to_str = lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ')

logger = logging.getLogger(__name__)

# credentials for the dataset
UN_CMEMS = os.environ['UN_CMEMS']
PW_CMEMS = os.environ['PW_CMEMS']
UN_RDA = os.environ['UN_RDA']
PW_RDA = os.environ['PW_RDA']

WIND_VAR_LIST = ['surface_downward_eastward_stress', 'wind_stress_divergence', 'northward_wind', 'sampling_length',
                 'wind_speed_rms', 'wind_vector_curl',
                 'northward_wind_rms', 'eastward_wind', 'wind_speed', 'wind_vector_divergence', 'wind_stress',
                 'wind_stress_curl', 'eastward_wind_rms', 'surface_type',
                 'surface_downward_northward_stress']

WAVE_VAR_LIST = ['VHM0_WW', 'VMDR_SW2', 'VMDR_SW1', 'VMDR', 'VTM10', 'VTPK', 'VPED',
                 'VTM02', 'VMDR_WW', 'VTM01_SW2', 'VHM0_SW1',
                 'VTM01_SW1', 'VSDX', 'VSDY', 'VHM0', 'VTM01_WW', 'VHM0_SW2']

PHY_VAR_LIST = ['vo', 'thetao', 'uo', 'zos', 'utotal', 'vtide', 'utide', 'vtotal']

DAILY_PHY_VAR_LIST = ['mlotst', 'siconc', 'usi', 'sithick', 'bottomT', 'vsi', 'so']

GFS_VAR_LIST = ['Temperature_surface', 'Wind_speed_gust_surface', 'u-component_of_wind_maximum_wind',
                'v-component_of_wind_maximum_wind', 'Dewpoint_temperature_height_above_ground',
                'U-Component_Storm_Motion_height_above_ground_layer',
                'V-Component_Storm_Motion_height_above_ground_layer', 'Relative_humidity_height_above_ground']


def get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    """
        retrieve all wave variables for a specific timestamp, latitude, longitude concidering
        the temporal resolution of the dataset to calculate interpolated values
    """
    logger.debug('obtaining GLOBAL_REANALYSIS_WAV dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    if date_lo < datetime(2019, 1, 1):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_WAV_001_032-TDS'
        product = 'global-reanalysis-wav-001-032'
    else:
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSIS_FORECAST_WAV_001_027-TDS'
        product = 'global-analysis-forecast-wav-001-027'
    dataset_temporal_resolution = 180

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

    data = try_get_data(url)
    return data


def get_global_phy_hourly(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    """
        retrieve <phy> including ... variables for a specific timestamp, latitude, longitude considering
        the temporal resolution of the dataset to calculate interpolated values
    """

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
    time.sleep(1)

    url = base_url + '&product=' + products[1] + '&product=global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh' + \
          '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                         y_lo,
                                                                                                         y_hi,
                                                                                                         date_to_str(
                                                                                                             t_lo)
                                                                                                         , date_to_str(
                  t_hi), z_lo, z_hi)

    data1 = try_get_data(url)
    return xr.combine_by_coords([data, data1.drop_vars(['uo', 'vo', 'vsdx', 'vsdy'])])


def try_get_data(url):
    try:
        CheckConnection.is_online()
        url_auth = authenticate_CAS_for_URL(url, UN_CMEMS, PW_CMEMS)
        CheckConnection.is_online()
        bytes_data = open_url(url_auth).read()
        CheckConnection.is_online()
        return xr.open_dataset(bytes_data)
    except Exception as e:
        raise ValueError('Error:', BeautifulSoup(bytes_data, 'html.parser').find('p', {"class": "error"}), 'Request: ',
                         url)


def get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining WIND_GLO_WIND_L4_NRT_OBSERVATIONS dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    CheckConnection.set_url('nrt.cmems-du.eu')
    base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
    service = 'WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004-TDS'
    product = 'CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE'
    dataset_temporal_resolution = 360
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
    data = try_get_data(url)
    return data


def get_cached(dataset, date, lat, lon, name):
    if name in ['wave', 'phy']:
        df = dataset.interp(longitude=[lon], latitude=[lat], time=[date], method='linear').to_dataframe()
    elif name == 'wind':
        df = dataset.interp(lon=[lon], lat=[lat], time=[date], method='linear').to_dataframe()
    elif name == 'phy_daily':
        df = dataset.sel(longitude=[lon], latitude=[lat], time=[date], method='nearest').to_dataframe()
        df.drop(columns=['thetao', 'uo', 'vo', 'zos'], inplace=True)
    elif name == 'gfs50':
        df = dataset.sel(lon=[lon], lat=[lat], time=[date], time1=[date], method='nearest').to_dataframe()
        df.drop(columns=['LatLon_Projection'], inplace=True)
    elif name == 'gfs25':
        lon = ((lon + 180) % 360) + 180
        df = dataset.interp(latitude=[lat], longitude=[lon], bounds_dim=1, time=[date]).to_dataframe()
        df = df[GFS_VAR_LIST]
    return np.ravel(df.values)


def get_GFS_25(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining GFS 0.25 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    x_arr_list = []
    base_url = 'https://rda.ucar.edu/thredds/catalog/files/g/ds084.1'
    CheckConnection.set_url('rda.ucar.edu')
    # calculate a day prior for midnight interpolation
    http_util.session_manager.set_session_options(auth=(UN_RDA, PW_RDA))
    start_date = datetime(date_lo.year, date_lo.month, date_lo.day) - timedelta(days=1)

    # consider the supported time range
    if start_date < datetime(2015, 1, 15): return xr.Dataset()

    start_cat = TDSCatalog(
        "%s/%s/%s%.2d%.2d/catalog.xml" % (base_url, start_date.year, start_date.year, start_date.month, start_date.day))
    ds_subset = start_cat.datasets[
        'gfs.0p25.%s%.2d%.2d18.f006.grib2' % (start_date.year, start_date.month, start_date.day)].subset()
    query = ds_subset.query().lonlat_box(north=lat_hi, south=lat_lo, east=lon_hi, west=lon_lo).variables(
        *GFS_VAR_LIST)
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
                ds_subset = end_cat.datasets[name].subset()
                query = ds_subset.query().lonlat_box(north=lat_hi, south=lat_lo, east=lon_hi, west=lon_lo).variables(
                    *GFS_VAR_LIST)
                CheckConnection.is_online()
                data = ds_subset.get_data(query)
                x_arr = xr.open_dataset(NetCDF4DataStore(data))
                if 'time1' in list(x_arr.coords):
                    x_arr = x_arr.rename({'time1': 'time'})
                x_arr_list.append(x_arr)
    return xr.combine_by_coords(x_arr_list).squeeze()


def get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining GFS 0.50 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    vars = {'0': [
        'Temperature_surface',
        'Wind_speed_gust_surface',
        'u-component_of_wind_maximum_wind',
        'v-component_of_wind_maximum_wind',
        'Dewpoint_temperature_height_above_ground',
        'U-Component_Storm_Motion_height_above_ground_layer',
        'V-Component_Storm_Motion_height_above_ground_layer',
        'Relative_humidity_height_above_ground'],

        '3': ['Precipitation_rate_surface_3_Hour_Average',
              'Temperature_surface',
              'Wind_speed_gust_surface',
              'u-component_of_wind_maximum_wind',
              'v-component_of_wind_maximum_wind',
              'Categorical_Freezing_Rain_surface_3_Hour_Average',
              'Categorical_Ice_Pellets_surface_3_Hour_Average',
              'Categorical_Rain_surface_3_Hour_Average',
              'Categorical_Snow_surface_3_Hour_Average',
              'Dewpoint_temperature_height_above_ground',
              'U-Component_Storm_Motion_height_above_ground_layer',
              'V-Component_Storm_Motion_height_above_ground_layer',
              'Relative_humidity_height_above_ground'],

        '6': ['Precipitation_rate_surface_6_Hour_Average',
              'Temperature_surface',
              'Wind_speed_gust_surface',
              'u-component_of_wind_maximum_wind',
              'v-component_of_wind_maximum_wind',
              'Dewpoint_temperature_height_above_ground',
              'U-Component_Storm_Motion_height_above_ground_layer',
              'V-Component_Storm_Motion_height_above_ground_layer',
              'Relative_humidity_height_above_ground']}

    base_url = 'https://www.ncei.noaa.gov/thredds/model-gfs-g4-anl-files-old/'
    CheckConnection.set_url('https://ncei.noaa.gov')

    dataList = []
    for day in range((date_hi - date_lo).days + 1):
        Hour_Averages = [0, 3, 6]
        for Hour_Average in Hour_Averages:
            for a in [0, 6, 12, 18]:
                attempts = 0
                while True:
                    try:
                        attempts += 1
                        dt = datetime(date_lo.year, date_lo.month, date_lo.day, a) + timedelta(days=day)
                        catalog = TDSCatalog(
                            '%s%s%.2d/%s%.2d%.2d/catalog.xml' % (
                                base_url, dt.year, dt.month, dt.year, dt.month, dt.day))
                        ds_name = 'gfsanl_4_%s%.2d%.2d_%.2d00_00%s.grb2' % (
                            dt.year, dt.month, dt.day, dt.hour, Hour_Average)
                        datasets = list(catalog.datasets)
                        if ds_name in datasets:
                            ncss = catalog.datasets[ds_name].subset()
                            query = ncss.query().time(dt + timedelta(hours=Hour_Average)).lonlat_box(
                                north=lat_hi, south=lat_lo, east=lon_hi, west=lon_lo).variables(
                                *vars[str(Hour_Average)])
                            data = ncss.get_data(query)
                            ncss.unit_handler(data)
                            x_arr = xr.open_dataset(NetCDF4DataStore(data))
                            dataList.append(x_arr)
                        else:
                            logger.warning('dataset %s is not found' % ds_name)
                        break
                    except Exception as e:
                        CheckConnection.is_online()
                        time.sleep(1.5)
                        if attempts % 20 == 0:
                            logger.error(e)
                            logger.error('Filename %s - Failed connecting to GFS Server - number of attempts: %d' % (
                                ds_name, attempts))
    return xr.merge(dataList, compat='override').squeeze().ffill(dim='time1').ffill(dim='time')


def get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining GLOBAL_ANALYSIS_FORECAST_PHY Daily dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    CheckConnection.set_url('nrt.cmems-du.eu')
    base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload&service=GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS'
    product = 'global-analysis-forecast-phy-001-024'

    t_lo = datetime(date_lo.year, date_lo.month, date_lo.day, 12)
    t_hi = datetime(date_hi.year, date_hi.month, date_hi.day, 12) + timedelta(days=1)

    # coordinates
    y_lo = float(lat_lo)
    y_hi = float(lat_hi)
    x_lo = float(lon_lo)
    x_hi = float(lon_hi)

    # depth
    z_hi = 0.50
    z_lo = 0.49

    url = base_url + '&product=' + product + '&product=global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh' + \
          '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                         y_lo,
                                                                                                         y_hi,
                                                                                                         date_to_str(
                                                                                                             t_lo)
                                                                                                         , date_to_str(
                  t_hi), z_lo, z_hi)
    data = try_get_data(url)
    return data


def append_to_csv(in_path, out_path):
    # get extracted AIS data and remove index column
    df = pd.read_csv(in_path, parse_dates=['BaseDateTime'], date_parser=str_to_date)
    df.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)

    # retrieve the data for each file once
    lat_hi = df.LAT.max()
    lon_hi = df.LON.max()

    lat_lo = df.LAT.min()
    lon_lo = df.LON.min()

    date_lo = df.BaseDateTime.min()
    date_hi = df.BaseDateTime.max()

    ds = get_global_phy_hourly(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    df[PHY_VAR_LIST] = df.apply(
        lambda x: get_cached(ds, x.BaseDateTime, x.LAT, x.LON, 'phy'), axis=1).apply(pd.Series)

    ds = get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    df[WIND_VAR_LIST] = df.apply(
        lambda x: get_cached(ds, x.BaseDateTime, x.LAT, x.LON, 'wind'), axis=1).apply(pd.Series)

    ds = get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    df[WAVE_VAR_LIST] = df.apply(
        lambda x: get_cached(ds, x.BaseDateTime, x.LAT, x.LON, 'wave'), axis=1).apply(pd.Series)

    ds = get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    df[DAILY_PHY_VAR_LIST] = df.apply(
        lambda x: get_cached(ds, x.BaseDateTime, x.LAT, x.LON, 'phy_daily'), axis=1).apply(pd.Series)

    ds = get_GFS_25(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    df[GFS_VAR_LIST] = df.apply(
        lambda x: get_cached(ds, x.BaseDateTime, x.LAT, x.LON, 'gfs25'), axis=1).apply(pd.Series)

    df.to_csv(out_path)


def append_environment_data(year, min_time_interval, work_dir):
    src_csv_path = Path(work_dir, str(year) + '_filtered_%s' % min_time_interval)
    output_csv_path = Path(work_dir, str(year) + '_merged_%s' % min_time_interval)
    Path(output_csv_path).mkdir(parents=True, exist_ok=True)
    csv_list = check_dir(src_csv_path)
    for file in csv_list:
        if Path(output_csv_path, file).exists(): continue
        logger.debug('append_environment_data in file %s' % str(Path(src_csv_path, file)))
        append_to_csv(Path(src_csv_path, file), Path(output_csv_path, file))
