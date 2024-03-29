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
import logging
import random
import sys

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from EnvironmentalData.weather import get_GFS, get_global_wave, get_global_phy_daily, get_global_wind, select_grid_point
from utilities import helper_functions

logger = logging.getLogger(__name__)


def validate_random_rows(df: pd.DataFrame, num_of_rows=10):
    df = df.dropna(how='all').fillna(value=0)
    gfs, wave, phy, wind = [], [], [], []
    for i in range(num_of_rows):
        if len(df) > 1:
            rand_row = df.loc[[random.randint(0, len(df) - 1)]]
            lat = rand_row.LAT.values[0]
            lon = rand_row.LON.values[0]
            date = pd.to_datetime(str(rand_row.BaseDateTime.values[0])).replace(tzinfo=None)
            logger.debug('Validating the interpolation of AIS point with LAT = %s, LON = %s, Timestamp = %s' % (
                str(lat), str(lon), str(date)))
            gfs_data = select_grid_point(*get_GFS(date, date, lat, lat, lon, lon), date, lat, lon)
            gfs.append(sum(cosine_similarity(rand_row[gfs_data.columns].values, gfs_data.values))[0])
            wave_data = select_grid_point(*get_global_wave(date, date, lat, lat, lon, lon), date, lat, lon)
            wave.append(sum(cosine_similarity(rand_row[wave_data.columns].values, wave_data.values))[0])

            phy_data = select_grid_point(*get_global_phy_daily(date, date, lat, lat, lon, lon), date, lat, lon)
            phy.append(sum(cosine_similarity(rand_row[phy_data.columns].values, phy_data.values))[0])
            wind_data = select_grid_point(*get_global_wind(date, date, lat, lat, lon, lon), date, lat, lon)
            wind.append(sum(cosine_similarity(rand_row[wind_data.columns].values, wind_data.values))[0])
    logger.debug('Avg. Similarity gfs %.2f' % sum(gfs) / len(gfs))
    logger.debug('Avg. Similarity wave %.2f' % sum(wave) / len(wave))
    logger.debug('Avg. Similarity wind %.2f' % sum(wind) / len(wind))
    logger.debug('Avg. Similarity phy %.2f' % sum(phy) / len(phy))


if __name__ == '__main__':
    validate_random_rows(
        pd.read_csv(sys.argv[1], parse_dates=['BaseDateTime'], date_parser=helper_functions.str_to_date))
