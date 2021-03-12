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

# utils to convert dates
from check_connection import CheckConnection

str_to_date = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
date_to_str = lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ')
import os

# credentials for the dataset
UN_CMEMS = os.environ['UN_CMEMS']
PW_CMEMS = os.environ['PW_CMEMS']


def get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    """
        retrieve all wave variables for a specific timestamp, latitude, longitude concidering
        the temporal resolution of the dataset to calculate interpolated values
    """
    if date_lo < datetime(2019, 1, 1):
        base_url = 'http://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_WAV_001_032-TDS'
        product = 'global-reanalysis-wav-001-032'
    else:
        base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
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
    base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload&service=GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS'
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
    return data, data1


def try_get_data(url):
    try:
        CheckConnection.is_online()
        url_auth = authenticate_CAS_for_URL(url, UN_CMEMS, PW_CMEMS)
        CheckConnection.is_online()
        bytes_data = open_url(url_auth).read()
        CheckConnection.is_online()
        return xr.open_dataset(bytes_data)
    except Exception as e:
        # print the error tag from html
        raise ValueError('Error:', BeautifulSoup(bytes_data, 'html.parser').find('p', {"class": "error"}), 'Request: ',
                         url)


def get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi):
    base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
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
    if name in ['wave', 'phy_0', 'phy_1']:
        df = dataset.interp(longitude=[lon], latitude=[lat], time=[date], method='linear').to_dataframe()
    elif name == 'wind':
        df = dataset.interp(lon=[lon], lat=[lat], time=[date], method='linear').to_dataframe()
    if name == 'phy_1':
        df.drop(columns=['uo', 'vo'], inplace=True)
    return np.ravel(df.values), list(df.columns)

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

    # the datasets could have different resolutions therefore get each separately
    wind_dataset = get_global_wind(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    wave_dataset = get_global_wave(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)
    phy_0_dataset, phy_1_dataset = get_global_phy_hourly(date_lo, date_hi, lat_lo, lat_hi, lon_lo, lon_hi)

    # define new columns for the output dataframe
    cols = list(df.columns)
    data_list = []
    for x in df.values:
        date, lat, lon = x[:3]

        wind_val, wind_cols = get_cached(wind_dataset, date, lat, lon, 'wind')

        wave_val, wave_cols = get_cached(wave_dataset, date, lat, lon, 'wave')

        phy_0_val, phy_cols_0 = get_cached(phy_0_dataset, date, lat, lon, 'phy_0')

        phy_1_val, phy_cols_1 = get_cached(phy_1_dataset, date, lat, lon, 'phy_1')
        data_list.append(np.concatenate([x, wind_val, wave_val, phy_0_val, phy_1_val]))
    pd.DataFrame(data_list, columns=cols + wind_cols + wave_cols + phy_cols_0 + phy_cols_1).to_csv(
        out_path)


def append_environment_data(year, min_time_interval):
    src_csv_path = Path(str(year) + '_filtered_%s' % min_time_interval)
    output_csv_path = Path(str(year) + '_merged_%s' % min_time_interval)
    Path(output_csv_path).mkdir(parents=True, exist_ok=True)
    csv_list = check_dir(src_csv_path)
    for file in csv_list:
        if Path(output_csv_path, file).exists(): continue
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),'  -', Path(src_csv_path, file))
        append_to_csv(Path(src_csv_path, file), Path(output_csv_path, file))
