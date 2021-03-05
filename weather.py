import pandas as pd
import requests
import numpy as np
from motu_utils.utils_cas import authenticate_CAS_for_URL
from motu_utils.utils_http import open_url
import xarray as xr
from datetime import datetime, timezone, timedelta
import time
from pathlib import Path
from bs4 import BeautifulSoup
import sys
from ais import download_AIS, check_dir

# utils to convert dates
str_to_date = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
date_to_str = lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ')


# credentials for the dataset
# UN_CMEMS =
# PW_CMEMS =


def get_global_wave(date, lat, lon):
    """
        retrieve all wave variables for a specific timestamp, latitude, longitude concidering
        the temporal resolution of the dataset to calculate interpolated values
    Parameters
    ----------
    :param date: datetime object
                datetime as a date object
    :param lat: str, float
                latitude
    :param lon: str, float
                longitude
    """
    if date < datetime(2019, 1, 1):
        base_url = 'http://my.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_REANALYSIS_WAV_001_032-TDS'
        product = 'global-reanalysis-wav-001-032'
    else:
        base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
        service = 'GLOBAL_ANALYSIS_FORECAST_WAV_001_027-TDS'
        product = 'global-analysis-forecast-wav-001-027'

    dataset_temporal_resolution = 180
    y_lo = float(lat)
    y_hi = float(lat)
    x_lo = float(lon)
    x_hi = float(lon)

    time_in_min = (date.hour * 60) + date.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date - timedelta(minutes=rest)
    t_hi = date + timedelta(minutes=dataset_temporal_resolution - rest)
    url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
        x_lo, x_hi, y_lo,
        y_hi,
        date_to_str(
            t_lo),
        date_to_str(
            t_hi))
    url_auth = authenticate_CAS_for_URL(url, UN_CMEMS, PW_CMEMS)
    bytes_data = open_url(url_auth).read()

    try:
        data = xr.open_dataset(bytes_data)
    except:
        # print the error tag from html
        raise ValueError('Error:', BeautifulSoup(bytes_data, 'html.parser').find('p', {"class": "error"}), 'Request: ',
                         url)

    df = data.to_dataframe().sort_values(by="time")
    # temporal interpolation
    alpha = rest / dataset_temporal_resolution
    return np.ravel((1 - alpha) * df.iloc[0] + (alpha * df.iloc[1])), list(df.columns)


def get_global_phy_hourly(date, lat, lon, product):
    """
        retrieve <phy> including ... variables for a specific timestamp, latitude, longitude considering
        the temporal resolution of the dataset to calculate interpolated values
    Parameters
    ----------
        :param date: datetime object
        datetime as a date object
        :param lat: str, float
        latitude
        :param lon: str, float
        longitude
        :param product: int
        use 0 to select global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh
        use 1 to select global-analysis-forecast-phy-001-024-hourly-merged-uv
    """
    base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload&service' \
               '=GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS '
    products = ['global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh',
                'global-analysis-forecast-phy-001-024-hourly-merged-uv']
    dataset_temporal_resolution = 60
    time_in_min = (date.hour * 60) + date.minute
    rest = time_in_min % dataset_temporal_resolution

    # time starting at min 30 of each hour
    if date.minute >= 30:
        t_lo = date - timedelta(minutes=rest) + timedelta(minutes=30)
        t_hi = date + timedelta(minutes=(dataset_temporal_resolution - rest)) + timedelta(minutes=30)
        alpha = (rest - 30) / dataset_temporal_resolution
    else:
        t_lo = date - timedelta(minutes=rest) - timedelta(minutes=30)
        t_hi = date + timedelta(minutes=(dataset_temporal_resolution - rest)) - timedelta(minutes=30)
        alpha = (rest + 30) / dataset_temporal_resolution

    # coordinates
    y_lo = float(lat)
    y_hi = float(lat)
    x_lo = float(lon)
    x_hi = float(lon)

    # depth
    z_hi = 0.50
    z_lo = 0.49

    url = base_url + '&product=' + products[
        product] + '&product=global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh' + \
          '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&z_lo={6}&z_hi={7}&mode=console'.format(x_lo, x_hi,
                                                                                                         y_lo,
                                                                                                         y_hi,
                                                                                                         date_to_str(
                                                                                                             t_lo)
                                                                                                         , date_to_str(
                  t_hi), z_lo, z_hi)
    url_auth = authenticate_CAS_for_URL(url, UN_CMEMS, PW_CMEMS)
    bytes_data = open_url(url_auth).read()
    try:
        data = xr.open_dataset(bytes_data)
    except:
        # print the error tag from html
        raise ValueError('Error:', BeautifulSoup(bytes_data, 'html.parser').find('p', {"class": "error"}),
                         'Request: ', url)

    df = data.to_dataframe().sort_values(by="time")
    if product == 1:
        df.drop(columns=['uo', 'vo'], inplace=True)

    return np.ravel((1 - alpha) * df.iloc[0] + (alpha * df.iloc[1])), list(df.columns)


def get_global_wind(date, lat, lon):
    base_url = 'http://nrt.cmems-du.eu/motu-web/Motu?action=productdownload'
    service = 'WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004-TDS'
    product = 'CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE'
    dataset_temporal_resolution = 360
    time_in_min = (date.hour * 60) + date.minute
    rest = time_in_min % dataset_temporal_resolution
    t_lo = date - timedelta(minutes=rest)
    t_hi = date + timedelta(minutes=dataset_temporal_resolution - rest)

    # coordinates
    y_lo = float(lat)
    y_hi = float(lat)
    x_lo = float(lon)
    x_hi = float(lon)
    url = base_url + '&service=' + service + '&product=' + product + '&x_lo={0}&x_hi={1}&y_lo={2}&y_hi={3}&t_lo={4}&t_hi={5}&mode=console'.format(
        x_lo, x_hi, y_lo,
        y_hi,
        date_to_str(
            t_lo),
        date_to_str(
            t_hi))
    url_auth = authenticate_CAS_for_URL(url, UN_CMEMS, PW_CMEMS)
    bytes_data = open_url(url_auth).read()
    try:
        data = xr.open_dataset(bytes_data)
    except:
        # print the error tag from html
        raise ValueError('Error:', BeautifulSoup(bytes_data, 'html.parser').find('p', {"class": "error"}), 'Request: ',
                         url)
    df = data.to_dataframe().sort_values(by="time")

    # temporal interpolation
    alpha = rest / dataset_temporal_resolution
    return np.ravel((1 - alpha) * df.iloc[0] + (alpha * df.iloc[1])), list(df.columns)


def append_environment_data(year, min_time_interval):
    src_csv_path = Path(str(year) + '_filtered_%s' % min_time_interval)
    output_csv_path = Path(str(year) + '_merged')
    Path(output_csv_path).mkdir(parents=True, exist_ok=True)

    csv_list = check_dir(src_csv_path)

    for file in csv_list:

        # get extracted AIS data and remove index column
        df = pd.read_csv(Path(src_csv_path, file), parse_dates=['BaseDateTime'], date_parser=str_to_date)
        df = df[df['BaseDateTime'] >= datetime(2019, 1, 1, 3)]
        df.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)

        # define new columns for the output datafarme
        cols = list(df.columns)

        # check if already appended data to resume in case of disconnect or other errors
        data_list = []
        if Path(output_csv_path, file).exists():
            data_list = list(
                pd.read_csv(Path(output_csv_path, file)).drop(['Unnamed: 0'], axis=1, errors='ignore').values)

        # loop over the AIS data starting from the last index, where it has stopped
        last_index = len(data_list)
        if last_index < len(df):
            print('\n' + file)
            print('Resuming download from row %s ' % len(data_list))

        for x in df.values[last_index:]:
            last_index += 1
            sys.stdout.write("\rEntry index: %s/%s" % (last_index, len(df)))
            sys.stdout.flush()
            date, lat, lon = x[:3]

            wind, wind_cols = get_global_wind(date, lat, lon)

            wave, wave_cols = get_global_wave(date, lat, lon)

            phy_0, phy_cols_0 = get_global_phy_hourly(date, lat, lon, 0)

            phy_1, phy_cols_1 = get_global_phy_hourly(date, lat, lon, 1)

            data_list.append(np.concatenate([x, wind, wave, phy_0, phy_1]))
            pd.DataFrame(data_list, columns=cols + wind_cols + wave_cols + phy_cols_0 + phy_cols_1).to_csv(
                Path(output_csv_path, file))
