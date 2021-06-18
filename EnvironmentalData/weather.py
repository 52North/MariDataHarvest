import logging
import time
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from glob import glob
from motu_utils.utils_cas import authenticate_CAS_for_URL
from motu_utils.utils_http import open_url
from siphon import http_util
from siphon.catalog import TDSCatalog
from xarray.backends import NetCDF4DataStore
import matplotlib.pyplot as plt
from utilities.check_connection import CheckConnection
from EnvironmentalData import config
from sklearn.cluster import DBSCAN
from scipy.signal import argrelextrema, peak_prominences
from mpl_toolkits.mplot3d import Axes3D
from utilities import helper_functions
from utilities.helper_functions import FileFailedException, Failed_Files, check_dir, create_csv
from mpl_toolkits.basemap import Basemap

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

GFS_50_VAR_LIST = ['Temperature_surface', 'u-component_of_wind_maximum_wind',
                   'v-component_of_wind_maximum_wind', 'U-Component_Storm_Motion_height_above_ground_layer',
                   'V-Component_Storm_Motion_height_above_ground_layer',
                   'Relative_humidity_height_above_ground']


def get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
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
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/WAV/GLOBAL_ANALYSIS_FORECAST_WAV_001_027'
        offset = 0.1
    elif date_lo >= datetime(1993, 1, 1, 6):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_WAV_001_032-TDS'
        product = 'global-reanalysis-wav-001-032'
        VM_FOLDER = '/eodata/CMEMS/REP/GLO/WAV/GLOBAL_REANALYSIS_WAV_001_032'
        offset = 0.2
    else:
        raise ValueError('Out of Range values')
    # time lower
    time_in_min = (date_lo.hour * 60) + date_lo.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date_lo - timedelta(minutes=rest)

    # time upper
    time_in_min = (date_hi.hour * 60) + date_hi.minute
    rest = time_in_min % dataset_temporal_resolution
    t_hi = date_hi + timedelta(minutes=dataset_temporal_resolution - rest)

    y_lo = float(lat_lo) - offset
    y_hi = float(lat_hi) + offset
    x_lo = float(lon_lo) - offset
    x_hi = float(lon_hi) + offset

    if Path(VM_FOLDER).exists():
        logger.debug('Accessing local data %s' % VM_FOLDER)
        datasets_paths = []
        for day in range((t_hi - t_lo).days + 1):
            dt = t_lo + timedelta(day)
            path = Path(VM_FOLDER, '%s' % dt.year, '%.2d' % dt.month, '%.2d' % dt.day, '*.nc')
            dataset = list(glob(str(path)))
            if len(dataset) > 0:
                datasets_paths.append(sorted(dataset)[0])
        ds_nc = xr.open_mfdataset(datasets_paths)
        if ds_nc.coords['latitude'].values[0] == ds_nc.coords['latitude'].max():
            tmp = y_lo
            y_lo = y_hi
            y_hi = tmp
        if ds_nc.coords['longitude'].values[0] == ds_nc.coords['longitude'].max():
            tmp = x_lo
            x_lo = x_hi
            x_hi = tmp
        dataset = ds_nc.sel(longitude=slice(x_lo, x_hi), latitude=slice(y_lo, y_hi),
                            time=slice(t_lo, t_hi)).compute()
    else:
        url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
            x_lo, x_hi, y_lo,
            y_hi,
            helper_functions.date_to_str(
                t_lo),
            helper_functions.date_to_str(
                t_hi))

        dataset = try_get_data(url)
    return dataset, 'wave'


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
        raise ValueError('Error:', e, 'Request: ', url)


def get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining WIND_GLO_WIND_L4_NRT_OBSERVATIONS dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    # offset according to the dataset resolution
    offset = 0.25
    dataset_temporal_resolution = 360
    # TODO Split request if date_lo and date_hi intersect with dataset's time boundaries
    if date_lo >= datetime(2018, 1, 1, 6):
        if (date_lo + timedelta(days=2)).date() > date.today() or (date_hi + timedelta(days=2)).date() > date.today():
            raise ValueError('Out of Range values')
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004-TDS'
        product = 'CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE'
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/WIN/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004'

    elif date_lo >= datetime(1992, 1, 1, 6):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_WIND_L4_REP_OBSERVATIONS_012_006-TDS'
        product = 'CERSAT-GLO-BLENDED_WIND_L4_REP-V6-OBS_FULL_TIME_SERIE'
        VM_FOLDER = '/eodata/CMEMS/REP/GLO/WIN/WIND_GLO_WIND_L4_REP_OBSERVATIONS_012_006'
    else:
        raise ValueError('Out of Range values')
    # time range
    time_in_min = (date_lo.hour * 60) + date_lo.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date_lo - timedelta(minutes=rest)  # extract the lower bound

    time_in_min = (date_hi.hour * 60) + date_hi.minute
    rest = time_in_min % dataset_temporal_resolution
    t_hi = date_hi + timedelta(minutes=dataset_temporal_resolution - rest)

    # coordinates bbox
    y_lo = float(lat_lo) - offset
    y_hi = float(lat_hi) + offset
    x_lo = float(lon_lo) - offset
    x_hi = float(lon_hi) + offset

    if Path(VM_FOLDER).exists():
        logger.debug('Accessing local data %s' % VM_FOLDER)
        datasets_paths = []
        for day in range((t_hi - t_lo).days + 1):
            dt = t_lo + timedelta(day)
            path = Path(VM_FOLDER, '%s' % dt.year, '%.2d' % dt.month, '%.2d' % dt.day, '*.nc')
            dataset = list(glob(str(path)))
            datasets_paths.extend(dataset)
        ds_nc = xr.open_mfdataset(datasets_paths)
        if ds_nc.coords['lat'].values[0] == ds_nc.coords['lat'].max():
            tmp = y_lo
            y_lo = y_hi
            y_hi = tmp
        if ds_nc.coords['lon'].values[0] == ds_nc.coords['lon'].max():
            tmp = x_lo
            x_lo = x_hi
            x_hi = tmp
        dataset = ds_nc.sel(lon=slice(x_lo, x_hi), lat=slice(y_lo, y_hi),
                            time=slice(t_lo, t_hi)).compute()
    else:
        url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
            x_lo, x_hi, y_lo,
            y_hi,
            helper_functions.date_to_str(
                t_lo),
            helper_functions.date_to_str(
                t_hi))
        dataset = try_get_data(url)
    return dataset, 'wind'


def get_GFS_prognoses(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi):
    offset = 0.25
    # check meridian bbox for GFS 50
    if lon_lo < 0 and lon_hi > 0:
        logger.debug(
            'GFS prognoses dataset: splitting the requested bbox over the prime meridian into LON [%s, %s] and LON [%s, %s]' % (
                lon_lo, -0.25, 0, lon_hi))
        a = get_GFS_prognoses(start_date, end_date, lat_lo, lat_hi, lon_lo, -0.25)
        b = get_GFS_prognoses(start_date, end_date, lat_lo, lat_hi, 0, lon_hi)
        return xr.combine_by_coords([a, b], coords=['longitude'], combine_attrs='override',
                                    compat='override').squeeze()

    end_cat = TDSCatalog(
        catalog_url="http://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/"
                    "Global_0p25deg/catalog.xml?dataset=grib/NCEP/GFS/Global_0p25deg/Best"
    )
    ds_subset = end_cat.datasets[0].subset()
    query = ds_subset.query().lonlat_box(north=lat_hi + offset, south=lat_lo - offset,
                                         east=lon_hi + offset,
                                         west=lon_lo - offset).time_range(end_date + timedelta(
        hours=0 if end_date == start_date + timedelta(days=1) else 3), end_date + timedelta(
        days=1)).variables(*GFS_25_VAR_LIST)
    CheckConnection.is_online()
    try:
        data = ds_subset.get_data(query)
        x_arr = xr.open_dataset(NetCDF4DataStore(data))[GFS_25_VAR_LIST]
        if 'time1' in list(x_arr.coords):
            x_arr = x_arr.rename({'time1': 'time', 'reftime1': 'reftime'})
        if 'height_above_ground' in list(x_arr.coords):
            x_arr = x_arr.rename({'height_above_ground': 'height_above_ground4'})
        if 'lon' in list(x_arr.coords):
            x_arr = x_arr.rename({'lon': 'longitude'})
        if 'lat' in list(x_arr.coords):
            x_arr = x_arr.rename({'lat': 'latitude'})
        return x_arr
    except Exception as e:
        print(e)
        logger.warning(traceback.format_exc())


def get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining GFS 0.25 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    start_date = datetime(date_lo.year, date_lo.month, date_lo.day) - timedelta(days=1)

    # consider the supported time range
    if datetime(2004, 3, 1) < start_date < datetime(2015, 1, 15):
        logger.debug('GFS 0.25 DATASET is out of supported range')
        return get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    elif datetime(2004, 3, 1) > start_date:
        raise ValueError('Out of Range values')

    # offset according to the dataset resolution
    offset = 0.25
    x_arr_list = []
    base_url = 'https://rda.ucar.edu/thredds/catalog/files/g/ds084.1'
    CheckConnection.set_url('rda.ucar.edu')
    # calculate a day prior for midnight interpolation
    http_util.session_manager.set_session_options(auth=(config['UN_RDA'], config['PW_RDA']))
    if (start_date + timedelta(days=4)).date() < date.today():
        try:
            start_cat = TDSCatalog(
                "%s/%s/%s%.2d%.2d/catalog.xml" % (
                    base_url, start_date.year, start_date.year, start_date.month, start_date.day))
            name = 'gfs.0p25.%s%.2d%.2d18.f006.grib2' % (start_date.year, start_date.month, start_date.day)
            ds_subset = start_cat.datasets[name].subset()
            query = ds_subset.query().lonlat_box(north=lat_hi + offset, south=lat_lo - offset, east=lon_hi + offset,
                                                 west=lon_lo - offset).variables(
                *GFS_25_VAR_LIST)
        except Exception as e:
            logger.warning('grib2 file error ' % e)
        CheckConnection.is_online()
        try:
            data = ds_subset.get_data(query)
            x_arr = xr.open_dataset(NetCDF4DataStore(data)).drop_dims(['bounds_dim'])[GFS_25_VAR_LIST]
            if 'time1' in list(x_arr.coords):
                x_arr = x_arr.rename({'time1': 'time'})
            x_arr_list.append(x_arr)
        except Exception as e:
            logger.warning('dataset %s is not complete' % name)
    for day in range((date_hi - date_lo).days + 1):
        end_date = datetime(date_lo.year, date_lo.month, date_lo.day) + timedelta(days=day)

        # check for real time dataset (today - 4) - 17 in the future
        if (end_date + timedelta(days=4)).date() > date.today():
            x_arr_list.append(get_GFS_prognoses(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi))
        else:
            end_cat = TDSCatalog(
                "%s/%s/%s%.2d%.2d/catalog.xml" % (base_url, end_date.year, end_date.year, end_date.month, end_date.day))
            for cycle in [0, 6, 12, 18]:
                for hours in [3, 6]:
                    name = 'gfs.0p25.%s%.2d%.2d%.2d.f0%.2d.grib2' % (
                        end_date.year, end_date.month, end_date.day, cycle, hours)
                    if name in list(end_cat.datasets):
                        ds_subset = end_cat.datasets[name].subset()
                        query = ds_subset.query().lonlat_box(north=lat_hi + offset,
                                                             south=lat_lo - offset,
                                                             east=lon_hi + offset,
                                                             west=lon_lo - offset).variables(*GFS_25_VAR_LIST)
                        CheckConnection.is_online()
                        try:
                            data = ds_subset.get_data(query)
                            x_arr = xr.open_dataset(NetCDF4DataStore(data)).drop_dims(['bounds_dim'])[GFS_25_VAR_LIST]
                            if 'time1' in list(x_arr.coords):
                                x_arr = x_arr.rename({'time1': 'time'})
                            x_arr_list.append(x_arr)
                        except Exception as e:
                            print(e)
                            logger.warning('dataset %s is not complete' % name)
                    else:
                        logger.warning('dataset %s is not found' % name)
    combined_xarrays = xr.combine_by_coords(x_arr_list,
                                            coords=['time', 'reftime'],
                                            combine_attrs='override',
                                            compat='override').squeeze().dropna('time')
    combined_xarrays['longitude'] = xr.where(combined_xarrays['longitude'] > 180, combined_xarrays['longitude'] - 360,
                                             combined_xarrays['longitude'])
    return combined_xarrays, 'gfs'


def get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    # check meridian bbox for GFS 50
    if lon_lo < 0 and lon_hi > 0:
        logger.debug(
            'GFS 0.50 dataset: splitting the requested bbox over the prime meridian into LON [%s, %s] and LON [%s, %s]' % (
                lon_lo, -0.5, 0, lon_hi))
        a = get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, lon_lo, -0.5)[0]
        b = get_GFS_50(date_lo, date_hi, lat_lo, lat_hi, 0, lon_hi)[0]
        return xr.combine_by_coords([a, b], coords=['lon'], combine_attrs='override',
                                    compat='override').squeeze(), 'gfs_50'
    logger.debug('obtaining GFS 0.50 dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    base_url = 'https://www.ncei.noaa.gov/thredds/model-gfs-g4-anl-files-old/'
    CheckConnection.set_url('ncei.noaa.gov')
    # offset according to the dataset resolution
    offset = 0.5
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
                            query = ds_subset.query().lonlat_box(north=lat_hi + offset, south=lat_lo - offset,
                                                                 east=lon_hi + offset, west=lon_lo - offset).variables(
                                *GFS_50_VAR_LIST)

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
                        if attempts > 15:
                            raise e
                        time.sleep(2)

    combined_xarrays = xr.combine_by_coords(x_arr_list, coords=['time'], combine_attrs='override',
                                            compat='override').squeeze()
    combined_xarrays['lon'] = xr.where(combined_xarrays['lon'] > 180, combined_xarrays['lon'] - 360,
                                       combined_xarrays['lon'])
    return combined_xarrays, 'gfs_50'


def get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    logger.debug('obtaining GLOBAL_ANALYSIS_FORECAST_PHY Daily dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))
    # offset according to the dataset resolution
    offset = 0.1
    if date_lo >= datetime(2019, 1, 2):
        CheckConnection.set_url('nrt.cmems-du.eu')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS'
        product = 'global-analysis-forecast-phy-001-024'
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/PHY/GLOBAL_ANALYSIS_FORECAST_PHY_001_024'
        NRT_FLAG = True
    elif date_lo >= datetime(1993, 1, 2):
        CheckConnection.set_url('my.cmems-du.eu')
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_PHY_001_030-TDS'
        product = 'global-reanalysis-phy-001-030-daily'
        VM_FOLDER = '/eodata/CMEMS/REP/GLO/PHY/GLOBAL_REANALYSIS_PHY_001_030'
        NRT_FLAG = False
    else:
        raise ValueError('Out of Range values')
    # time range
    t_lo = datetime(date_lo.year, date_lo.month, date_lo.day, 12) - timedelta(days=1)
    t_hi = datetime(date_hi.year, date_hi.month, date_hi.day, 12) + timedelta(days=1)

    # coordinates bbox
    y_lo = float(lat_lo) - offset
    y_hi = float(lat_hi) + offset
    x_lo = float(lon_lo) - offset
    x_hi = float(lon_hi) + offset

    # depth
    z_hi = 0.50
    z_lo = 0.49

    if Path(VM_FOLDER).exists():
        logger.debug('Accessing local data %s' % VM_FOLDER)
        datasets_paths = []
        for day in range((t_hi - t_lo).days + 1):
            dt = t_lo + timedelta(day)
            path = Path(VM_FOLDER, '%s' % dt.year, '%.2d' % dt.month, '%.2d' % dt.day,
                        'mercatorpsy4v3r1_gl12_mean_%s%.2d%.2d_*.nc' % (dt.year, dt.month, dt.day) if NRT_FLAG
                        else 'mercatorglorys12v1_gl12_mean_%s%.2d%.2d_*.nc' % (dt.year, dt.month, dt.day))
            dataset = list(glob(str(path)))
            if len(dataset) > 0:
                datasets_paths.append(dataset[0])

        ds_nc = xr.open_mfdataset(datasets_paths)
        if ds_nc.coords['latitude'].values[0] == ds_nc.coords['latitude'].max():
            tmp = y_lo
            y_lo = y_hi
            y_hi = tmp
        if ds_nc.coords['longitude'].values[0] == ds_nc.coords['longitude'].max():
            tmp = x_lo
            x_lo = x_hi
            x_hi = tmp
        dataset = ds_nc.sel(longitude=slice(x_lo, x_hi), latitude=slice(y_lo, y_hi),
                            time=slice(t_lo, t_hi), depth=slice(z_lo, z_hi)).compute()
    else:
        url = base_url + '&service=' + service + '&product=' + product + \
              '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                             y_lo,
                                                                                                             y_hi,
                                                                                                             helper_functions.date_to_str(
                                                                                                                 t_lo)
                                                                                                             ,
                                                                                                             helper_functions.date_to_str(
                                                                                                                 t_hi),
                                                                                                             z_lo, z_hi)
        dataset = try_get_data(url)
    return dataset, 'phy'


def interpolate(ds: xr.Dataset, ds_name: str, time_points: xr.DataArray, lat_points: xr.DataArray,
                lon_points: xr.DataArray, var_list: list):
    if ds_name == 'wave':
        res = ds.interp(longitude=lon_points, latitude=lat_points, time=time_points).to_dataframe()[
            var_list].reset_index(drop=True)
    elif ds_name == 'wind':
        res = ds.interp(lon=lon_points, lat=lat_points, time=time_points).to_dataframe()[var_list].reset_index(
            drop=True)
    elif ds_name == 'gfs':
        # b = xr.DataArray([1] * len(lon_points))
        res = ds.interp(longitude=lon_points, latitude=lat_points, time=time_points).to_dataframe()[
            var_list].reset_index(drop=True)
    elif ds_name == 'gfs_50':
        gfs_var = [var for var in var_list if var in GFS_50_VAR_LIST]  # skipping missing variables in older datasets
        res = ds.interp(lon=lon_points, lat=lat_points, time=time_points).to_dataframe()[gfs_var].reset_index(
            drop=True)
    elif ds_name == 'phy':
        res = ds.interp(longitude=lon_points, latitude=lat_points, time=time_points).to_dataframe()[
            var_list].reset_index(drop=True)
    ds.close()
    return res


def select_grid_point(ds: xr.Dataset, ds_name: str, time_point: datetime, lat_point: float,
                      lon_point: float) -> pd.DataFrame:
    if ds_name == 'wave':
        res = ds.sel(longitude=lon_point, latitude=lat_point, method='nearest').to_dataframe()[
            WAVE_VAR_LIST].reset_index(drop=True)
    elif ds_name == 'wind':
        res = ds.sel(lon=lon_point, lat=lat_point, method='nearest').to_dataframe()[WIND_VAR_LIST].reset_index(
            drop=True)
    elif ds_name == 'gfs':
        res = ds.sel(longitude=lon_point, latitude=lat_point, time=time_point, method='nearest').to_dataframe()[
            GFS_25_VAR_LIST].reset_index(
            drop=True)
    elif ds_name == 'gfs_50':
        res = ds.sel(lon=lon_point, lat=lat_point, time=time_point, method='nearest').to_dataframe()[
            GFS_50_VAR_LIST].reset_index(
            drop=True)
    elif ds_name == 'phy':
        res = ds.sel(longitude=lon_point, latitude=lat_point, time=time_point, method='nearest').to_dataframe()[
            DAILY_PHY_VAR_LIST].reset_index(drop=True)
    ds.close()
    return res.fillna(value=0)


def append_to_csv(in_path: Path, out_path: Path = None, gfs=None, wind=None, wave=None, phy=None, col_dict={},
                  metadata={}):
    if not bool(col_dict):
        # default for marinecadastre
        col_dict = {'time': 'BaseDateTime', 'lat': 'LAT', 'lon': 'LON'}
    if phy is None:
        phy = DAILY_PHY_VAR_LIST
    if wave is None:
        wave = WAVE_VAR_LIST
    if wind is None:
        wind = WIND_VAR_LIST
    if gfs is None:
        gfs = GFS_25_VAR_LIST
    logger.debug('append_environment_data in file %s' % in_path)

    header = True
    try:
        for df_chunk in pd.read_csv(in_path, parse_dates=[col_dict['time']], date_parser=helper_functions.str_to_date,
                                    chunksize=helper_functions.CHUNK_SIZE):
            if len(df_chunk) > 1:

                date_lo = df_chunk[col_dict['time']].min()
                date_hi = df_chunk[col_dict['time']].max()

                # remove index column if exists
                df_chunk.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)

                if (date_hi - date_lo).days > 15:
                    # discrete requests according to local maxima of the 1d data
                    lon_maxima = argrelextrema(np.array(df_chunk[col_dict['lon']].values), np.greater,
                                               order=(len(df_chunk) // 12))[0]
                    lat_maxima = argrelextrema(np.array(df_chunk[col_dict['lat']].values), np.greater,
                                               order=(len(df_chunk) // 12))[0]
                    local_maxima_index = lat_maxima if len(lat_maxima) > len(lon_maxima) else lon_maxima
                else:
                    df_chunk.sort_values([col_dict['lat']], inplace=True)
                    local_maxima_index = argrelextrema(np.array(df_chunk[col_dict['lon']].values), np.greater,
                                                       order=(len(df_chunk) // 12))[0]
                start_index = 0
                for index in list(local_maxima_index) + [-1]:
                    df_chunk_sub = df_chunk[start_index:index]
                    start_index = index

                    # retrieve the data for each file once
                    lat_hi = df_chunk_sub[col_dict['lat']].max()
                    lon_hi = df_chunk_sub[col_dict['lon']].max()

                    lat_lo = df_chunk_sub[col_dict['lat']].min()
                    lon_lo = df_chunk_sub[col_dict['lon']].min()

                    date_lo = df_chunk_sub[col_dict['time']].min()
                    date_hi = df_chunk_sub[col_dict['time']].max()

                    if abs(lat_hi - lat_lo) + abs(lon_hi - lon_lo) > 150 or (date_hi - date_lo).days > 30:
                        raise ValueError('exceeds temporal or spatial extent.')

                    # query parameters
                    time_points = xr.DataArray(list(df_chunk_sub[col_dict['time']].values))
                    lat_points = xr.DataArray(list(df_chunk_sub[col_dict['lat']].values))
                    lon_points = xr.DataArray(list(df_chunk_sub[col_dict['lon']].values))
                    df_chunk_sub.reset_index(drop=True, inplace=True)
                    if len(gfs) > 0:
                        df_chunk_sub = pd.concat(
                            [df_chunk_sub,
                             interpolate(*get_GFS(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi), time_points,
                                         lat_points, lon_points, gfs)], axis=1)

                    if len(phy) > 0:
                        df_chunk_sub = pd.concat(
                            [df_chunk_sub,
                             interpolate(*get_global_phy_daily(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi),
                                         time_points,
                                         lat_points, lon_points, phy)], axis=1)

                    if len(wind) > 0:
                        df_chunk_sub = pd.concat(
                            [df_chunk_sub,
                             interpolate(*get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi),
                                         time_points,
                                         lat_points,
                                         lon_points, wind)], axis=1)

                    if len(wave) > 0:
                        df_chunk_sub = pd.concat(
                            [df_chunk_sub,
                             interpolate(*get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi),
                                         time_points,
                                         lat_points,
                                         lon_points, wave)], axis=1)
                    if bool(metadata) and header:
                        create_csv(df_chunk_sub, metadata, out_path, index=False)
                        header = False
                    else:
                        df_chunk_sub.to_csv(out_path, mode='a', header=header, index=False)
                plt.show()

    except Exception as e:
        # discard the file in case of an error to resume later properly
        if out_path:
            out_path.unlink(missing_ok=True)
            raise FileFailedException(out_path.name, e)
        raise e
