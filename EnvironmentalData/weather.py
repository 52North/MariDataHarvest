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
from datetime import datetime, timedelta, date, timezone
from glob import glob
from pathlib import Path
import logging
import time
import traceback

from motu_utils.utils_cas import authenticate_CAS_for_URL
from motu_utils.utils_http import open_url
from pydap.cas.get_cookies import setup_session
from pydap.client import open_url as open_url_pydap
from scipy.signal import argrelextrema
from siphon import http_util
from siphon.catalog import TDSCatalog
from xarray.backends import NetCDF4DataStore
import numpy as np
import pandas as pd
import requests.exceptions
import xarray as xr

from EnvironmentalData import config
from utilities import helper_functions

logger = logging.getLogger(__name__)

WAVE_VAR_DICT = {
        'VHM0_WW':		'sea_surface_wind_wave_significant_height',
        'VMDR_SW2':		'sea_surface_secondary_swell_wave_from_direction',
        'VMDR_SW1':		'sea_surface_primary_swell_wave_from_direction',
        'VMDR':		'sea_surface_wave_from_direction',
        'VTM10':		'sea_surface_wave_mean_period_from_variance_spectral_density_inverse_frequency_moment',
        'VTPK':		'sea_surface_wave_period_at_variance_spectral_density_maximum',
        'VPED':		'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
        'VTM02':		'sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment',
        'VMDR_WW':		'sea_surface_wind_wave_from_direction',
        'VTM01_SW2':		'sea_surface_secondary_swell_wave_mean_period',
        'VHM0_SW1':		'sea_surface_primary_swell_wave_significant_height',
        'VTM01_SW1':		'sea_surface_primary_swell_wave_mean_period',
        'VSDX':		'sea_surface_wave_stokes_drift_x_velocity',
        'VSDY':		'sea_surface_wave_stokes_drift_y_velocity',
        'VHM0':		'sea_surface_wave_significant_height',
        'VTM01_WW':		'sea_surface_wind_wave_mean_period'
}

WIND_VAR_DICT = {
        'northward_wind': 'stress-equivalent wind northward component at 10 m',
        'northward_wind_bias': 'scatterometer-model bias of stress-equivalent wind northward component at 10 m',
        'northward_wind_sdd': 'standard deviation of differences of stress-equivalent wind northward component at 10',
        'eastward_wind': 'stress-equivalent wind eastward component at 10 m',
        'eastward_wind_bias': 'scatterometer-model bias of stress-equivalent wind eastward component at 10 m',
        'eastward_wind_sdd': 'standard deviation of differences of stress-equivalent wind eastward component at 10',
        'wind_divergence': 'scatterometer-model bias of divergence of stress-equivalent wind at 10 m',
        'wind_divergence_bias': 'scatterometer-model bias of divergence of stress-equivalent wind at 10 m',
        'wind_divergence_dv': 'difference of scatterometer and model variances of divergence of stress-equivalent wind at 10 m',
        'wind_curl': 'curl of stress-equivalent wind at 10 m',
        'wind_curl_bias': 'scatterometer-model bias of curl of stress-equivalent wind at 10 m',
        'wind_curl_dv': 'difference of scatterometer and model variances of curl of stress-equivalent wind at 10 m',
        'eastward_stress': 'surface wind stress eastward component',
        'eastward_stress_bias': 'scatterometer-model bias of surface wind stress eastward component',
        'eastward_stress_sdd': 'standard deviation of differences of surface wind stress eastward component',
        'northward_stress': 'surface wind stress northward component',
        'northward_stress_bias': 'scatterometer-model bias of surface wind stress northward component',
        'northward_stress_sdd': 'standard deviation of differences of surface wind stress northward component',
        'stress_divergence': 'divergence of surface wind stress',
        'stress_divergence_bias': 'scatterometer-model bias of divergence of surface wind stress',
        'stress_divergence_dv': 'difference of scatterometer and model variances of divergence of surface wind stress',
        'stress_curl': 'rotation of surface wind stress',
        'stress_curl_bias': 'scatterometer-model bias of curl of surface wind stress',
        'stress_curl_dv': 'difference of scatterometer and model variances of curl of surface wind stress',
        'air_density': 'air density at 10 m',
        'number_of_observations': 'number of observations used for scatterometer-model bias',
        'number_of_observations_divcurl': 'number of observations used for scatterometer-model divergence and curl bias'
}

DAILY_PHY_VAR_DICT = {
        'thetao':'Potential Temperature',
        'vo':'Northward velocity',
        'uo':'Eastward velocity',
        'so':'Salinity',
        'zos':'Sea surface height'
}

GFS_25_VAR_DICT = {
        'Temperature_surface':'Temperature @ Ground or water surface',
        'Pressure_surface':'Pressure @ Ground or water surface',
        'Wind_speed_gust_surface':'Wind speed (gust) @ Ground or water surface',
        'u-component_of_wind_height_above_ground':'u-component of wind @ Specified height level above ground',
        'v-component_of_wind_height_above_ground':'v-component of wind @ Specified height level above ground',
        'u-component_of_wind_sigma':'u-component of wind @ Sigma level',
        'v-component_of_wind_sigma':'v-component of wind @ Sigma level',
        'u-component_of_wind_maximum_wind':'u-component of wind @ Maximum wind level',
        'v-component_of_wind_maximum_wind':'v-component of wind @ Maximum wind level',
        'Dewpoint_temperature_height_above_ground':'Dewpoint temperature @ Specified height level above ground',
        'Relative_humidity_height_above_ground':'Relative humidity @ Specified height level above ground',
        'U-Component_Storm_Motion_height_above_ground_layer':'U-Component Storm Motion @ Specified height level above ground layer',
        'V-Component_Storm_Motion_height_above_ground_layer':'V-Component Storm Motion @ Specified height level above ground layer'
}

def get_parameter_list(parameter_dict):
    keys_list = list(parameter_dict.keys())
    return list(parameter_dict)

WAVE_VAR_LIST = get_parameter_list(WAVE_VAR_DICT)
WIND_VAR_LIST = get_parameter_list(WIND_VAR_DICT)
DAILY_PHY_VAR_LIST = get_parameter_list(DAILY_PHY_VAR_DICT)
GFS_25_VAR_LIST = get_parameter_list(GFS_25_VAR_DICT)

def get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    """
        retrieve all wave variables for a specific timestamp, latitude, longitude concidering
        the temporal resolution of the dataset to calculate interpolated values
    """
    logger.debug('obtaining GLOBAL_REANALYSIS_WAV dataset for DATE [%s, %s] LAT [%s, %s] LON [%s, %s]' % (
        str(date_lo), str(date_hi), str(lat_lo), str(lat_hi), str(lon_lo), str(lon_hi)))

    first_nrt_datetime = get_first_cmems_datetime('cmems_mod_glo_wav_anfc_0.083deg_PT3H-i', 'nrt')
    dataset_temporal_resolution = 180
    # .replace does not modify date_lo in place but creates a new datetime object
    if date_lo.replace(tzinfo=timezone.utc) >= first_nrt_datetime:
        # nrt => near real time
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSISFORECAST_WAV_001_027-TDS'
        product = 'cmems_mod_glo_wav_anfc_0.083deg_PT3H-i'
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/WAV/GLOBAL_ANALYSIS_FORECAST_WAV_001_027'
        offset = 0.1
    elif date_lo >= datetime(1993, 1, 1, 6):
        # my => multi year
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_MULTIYEAR_WAV_001_032-TDS'
        product = 'cmems_mod_glo_wav_my_0.2_PT3H-i'
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
        url_auth = authenticate_CAS_for_URL(url, config['UN_CMEMS'], config['PW_CMEMS'])
        response = open_url(url_auth)
        read_bytes = response.read()
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
    first_nrt_datetime = get_first_cmems_datetime('cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H', 'nrt')
    # .replace does not modify date_lo in place but creates a new datetime object
    if date_lo.replace(tzinfo=timezone.utc) >= first_nrt_datetime:
        if (date_lo + timedelta(days=2)).date() > date.today() or (date_hi + timedelta(days=2)).date() > date.today():
            raise ValueError('Out of Range values')
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_PHY_L4_NRT_012_004-TDS'
        product = 'cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H'
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/WIN/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004'

    elif date_lo >= datetime(1992, 1, 1, 6):
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'WIND_GLO_PHY_L4_MY_012_006-TDS'
        product = 'cmems_obs-wind_glo_phy_my_l4_0.125deg_PT1H'
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
    if lon_lo < 0 < lon_hi:
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
    base_url = 'https://thredds.rda.ucar.edu/thredds/catalog/files/g/ds084.1'
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
                                                 west=lon_lo - offset).variables(*GFS_25_VAR_LIST)
        except Exception as e:
            # TODO be MORE specific regarding the errors to swallow and to not catch nearly all exceptions
            # e.g. do not catch ConnectionError
            # Exceptions are swallowed because the temporal offset at the beginning can result in ignorable errors
            if isinstance(e, requests.exceptions.ConnectionError):
                raise e
            else:
                logger.warning('grib2 file error: {}'.format(str(e)))
        try:
            data = ds_subset.get_data(query)
            x_arr = xr.open_dataset(NetCDF4DataStore(data)).drop_dims(['bounds_dim'])[GFS_25_VAR_LIST]
            if 'time1' in list(x_arr.coords):
                x_arr = x_arr.rename({'time1': 'time'})
            x_arr_list.append(x_arr)
        except Exception as e:
            logger.warning('Exception thrown: {}'.format(str(e)))
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
                        try:
                            data = ds_subset.get_data(query)
                            x_arr = xr.open_dataset(NetCDF4DataStore(data)).drop_dims(['bounds_dim'])[GFS_25_VAR_LIST]
                            if 'time1' in list(x_arr.coords):
                                x_arr = x_arr.rename({'time1': 'time'})
                            x_arr_list.append(x_arr)
                        except Exception as e:
                            logger.warning('Exception thrown: {}'.format(str(e)))
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
                        logger.error(str(e))
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
    first_nrt_datetime = get_first_cmems_datetime('cmems_mod_glo_phy_anfc_0.083deg_PT1H-m', 'nrt')
    offset = 0.1
    # .replace does not modify date_lo in place but creates a new datetime object
    if date_lo.replace(tzinfo=timezone.utc) >= first_nrt_datetime:
        base_url = 'https://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSISFORECAST_PHY_001_024-TDS'
        product = 'cmems_mod_glo_phy_anfc_0.083deg_PT1H-m'
        VM_FOLDER = '/eodata/CMEMS/NRT/GLO/PHY/GLOBAL_ANALYSIS_FORECAST_PHY_001_024'
        NRT_FLAG = True
    elif date_lo >= datetime(1993, 1, 2):
        base_url = 'https://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_MULTIYEAR_PHY_001_030-TDS'
        product = 'cmems_mod_glo_phy_my_0.083_P1D-m'
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
                  metadata={}, webapp=False):
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

                # date_lo = df_chunk[col_dict['time']].min()
                # date_hi = df_chunk[col_dict['time']].max()

                # remove index column if exists
                df_chunk.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)

                # if (date_hi - date_lo).days > 15:
                #     # discrete requests according to local maxima of the 1d data
                #     lon_maxima = argrelextrema(np.array(df_chunk[col_dict['lon']].values), np.greater,
                #                                order=(len(df_chunk) // 12))[0]
                #     lat_maxima = argrelextrema(np.array(df_chunk[col_dict['lat']].values), np.greater,
                #                                order=(len(df_chunk) // 12))[0]
                #     local_maxima_index = lat_maxima if len(lat_maxima) > len(lon_maxima) else lon_maxima
                # else:
                #     df_chunk.sort_values([col_dict['lat']], inplace=True)
                #     local_maxima_index = argrelextrema(np.array(df_chunk[col_dict['lon']].values), np.greater,
                #                                        order=(len(df_chunk) // 12))[0]
                # start_index = 0
                # for index in list(local_maxima_index) + [-1]:
                df_chunk_sub = df_chunk
                # start_index = index

                # retrieve the data for each file once
                lat_hi = df_chunk_sub[col_dict['lat']].max()
                lon_hi = df_chunk_sub[col_dict['lon']].max()

                lat_lo = df_chunk_sub[col_dict['lat']].min()
                lon_lo = df_chunk_sub[col_dict['lon']].min()

                date_lo = df_chunk_sub[col_dict['time']].min()
                date_hi = df_chunk_sub[col_dict['time']].max()

                if webapp and (date_hi - date_lo).days > 30:
                    error = 'Exceeds temporal extent: requested days exceed 30 days: {} - {} = {} days'.format(
                        date_hi, date_lo, (date_hi - date_lo).days
                    )
                    logger.debug(error)
                    raise ValueError(error)

                if webapp and abs(lat_hi - lat_lo) + abs(lon_hi - lon_lo) > 150:
                    error = 'Exceeds spatial extent: longitude and latitude extent combined exceed 150°: ' + \
                            'Lat: {}° - {}° = {}°; Lon: {}° - {}° = {}°; {}° + {}° = {}°'.format(
                                lat_hi, lat_lo, abs(lat_hi - lat_lo),
                                lon_hi, lon_lo, abs(lon_hi - lon_lo),
                                abs(lat_hi - lat_lo), abs(lon_hi - lon_lo), abs(lon_hi - lon_lo) + abs(lat_hi - lat_lo)
                            )
                    logger.debug(error)
                    raise ValueError(error)

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
                    helper_functions.create_csv(df_chunk_sub, metadata, out_path, index=False)
                    header = False
                else:
                    df_chunk_sub.to_csv(out_path, mode='a', header=header, index=False)
                    header = False  # TODO add metadata later
    except Exception as e:
        # discard the file in case of an error to resume later properly
        if out_path:
            out_path.unlink(missing_ok=True)
            raise helper_functions.FileFailedException(out_path.name, e)
        raise e


def get_cmems_data_store(product, product_type, username, password):
    cas_url = 'https://cmems-cas.cls.fr/cas/login'
    session = setup_session(cas_url, username, password)
    session.cookies.set("CASTGC", session.cookies.get_dict()['CASTGC'])
    url = f'https://{product_type}.cmems-du.eu/thredds/dodsC/{product}'
    try:
        data_store = xr.backends.PydapDataStore(open_url_pydap(url, session=session))
    except Exception as err:
        raise err
    return data_store

def get_first_cmems_datetime(product, product_type):
    first_cmems_datetime = datetime(2021, 1, 1, 3, 0, tzinfo=timezone.utc)
    try:
        data_store = get_cmems_data_store(product, product_type,
                                          config['UN_CMEMS'], config['PW_CMEMS'])
        ds = xr.open_dataset(data_store)
        first_cmems_datetime = helper_functions.convert_datetime(ds.time[0].values)
    except Exception as err:
        logger.warning(f"could not retrieve latest nrt date. Setting it to {str(first_cmems_datetime)}. Error message: {err}")
    finally:
        return first_cmems_datetime