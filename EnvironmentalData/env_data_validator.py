import random
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from utilities import helper_functions
from EnvironmentalData.weather import get_GFS, get_global_wave, get_global_phy_daily, get_global_wind, select_grid_point


def validate_random_row(df: pd.DataFrame):
    df = df.dropna(how='all').fillna(value=0)
    for i in range(10):
        if len(df) > 1:
            rand_row = df.loc[[random.randint(0, len(df) - 1)]]
            lat = rand_row.LAT.values[0]
            lon = rand_row.LON.values[0]
            date = pd.to_datetime(str(rand_row.BaseDateTime.values[0])).replace(tzinfo=None)
            print('Validating AIS point with LAT = %s, LON = %s, Timestamp = %s' % (
                str(lat), str(lon), str(date)))

            gfs = select_grid_point(*get_GFS(date, date, lat, lat, lon, lon), date, lat, lon)
            print(
                'Similarty gfs %.2f' % sum(cosine_similarity(rand_row[gfs.columns].values, gfs.values))[0])
            wave = select_grid_point(*get_global_wave(date, date, lat, lat, lon, lon), date, lat, lon)
            print(
                'Similarty wave %.2f' % sum(cosine_similarity(rand_row[wave.columns].values, wave.values))[0])

            phy = select_grid_point(*get_global_phy_daily(date, date, lat, lat, lon, lon), date, lat, lon)
            print(
                'Similarty phy %.2f' % sum(cosine_similarity(rand_row[phy.columns].values, phy.values))[0])
            wind = select_grid_point(*get_global_wind(date, date, lat, lat, lon, lon), date, lat, lon)
            print(
                'Similarty wind %.2f' % sum(cosine_similarity(rand_row[wind.columns].values, wind.values))[0])


in_path = 'C:\\Users\\Sufian\\PycharmProjects\\2019_merged_15\\AIS_2019_01_03.csv'
validate_random_row(pd.read_csv(in_path, parse_dates=['BaseDateTime'], date_parser=helper_functions.str_to_date))
