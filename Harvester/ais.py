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
from datetime import datetime
from pathlib import Path
import logging
import os
import shutil
import typing
import warnings
import zipfile

from bs4 import BeautifulSoup
import geopandas as gpd
import pandas as pd
import requests

from utilities.helper_functions import FileFailedException, Failed_Files, check_dir, CHUNK_SIZE

pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_files_list(year: int, exclude_to_resume: typing.List[str]) -> typing.List[str]:
    # url link to data
    url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{0}/".format(year)

    # request the html file
    html_text = requests.get(url).text

    # parse the html
    soup = BeautifulSoup(html_text, 'html.parser')

    # iterate over the <a> tags and save each in a list
    files = []
    for a in soup.find_all('a', href=True):
        if a.text and a.text.endswith('zip'):
            name = a['href'].split('.')[0]
            name = name.split('/')[-1] if len(name.split('/')) > 1 else name
            if name + '.csv' in exclude_to_resume + Failed_Files or name + '.gdb' in exclude_to_resume + Failed_Files or name + '.zip' in Failed_Files:
                continue
            files.append(a['href'])
    return files


def chunkify_gdb(gdb_file: Path, file_path: Path) -> None:
    end = CHUNK_SIZE
    start = 0
    header = True
    while True:
        gdf_chunk = gpd.read_file(gdb_file, rows=slice(start, end))
        if len(gdf_chunk) == 0: break
        gdf_chunk['LON'] = gdf_chunk.geometry.apply(lambda point: point.x)
        gdf_chunk['LAT'] = gdf_chunk.geometry.apply(lambda point: point.y)
        gdf_chunk.drop(columns=['geometry'], inplace=True)
        gdf_chunk.to_csv(file_path, mode='a', header=header, index=False)
        start = end
        end += CHUNK_SIZE
        header = False


def download_file(zipped_file_name: str, download_dir: Path, year: int) -> str:
    try:
        # url link to data
        url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{0}/".format(year)
        logger.info('downloading AIS file: %s' % zipped_file_name)

        # download zip file using wget with url and file name
        with requests.get(os.path.join(url, zipped_file_name), stream=True) as req:
            req.raise_for_status()
            zipped_file_name = zipped_file_name.split('/')[-1] if len(
                zipped_file_name.split('/')) > 1 else zipped_file_name
            with open(zipped_file_name, "wb") as handle:
                for chunk in req.iter_content(chunk_size=8192):
                    handle.write(chunk)
                handle.close()
        # extract each zip file into output directory then delete it
        with zipfile.ZipFile(zipped_file_name, 'r') as zip_ref:
            for f in zip_ref.infolist():
                if f.filename.endswith('.csv'):
                    f.filename = os.path.basename(f.filename)
                    file_name = f.filename
                    zip_ref.extract(f, download_dir)
                if str(Path(f.filename).parent).endswith('.gdb'):
                    zip_ref.extractall(download_dir)
                    name = str(Path(f.filename).parent)
                    gdb_file = Path(download_dir, name)
                    file_name = name.split('.')[0] + '.csv'
                    file_path = Path(download_dir, file_name)
                    try:
                        chunkify_gdb(gdb_file, file_path)
                    except Exception as e:
                        # discard the file in case of an error to resume later properly
                        if file_path:
                            file_path.unlink(missing_ok=True)
                        raise e
                    shutil.rmtree(gdb_file)
                    break
        os.remove(zipped_file_name)
        return file_name
    except Exception as e:
        raise FileFailedException(zipped_file_name, e)


def download_year_AIS(year: int, download_dir: Path) -> None:
    # create a directory named after the given year if not exist
    resume_download = []
    if download_dir.exists():
        resume_download = check_dir(download_dir)
    files = get_files_list(year, exclude_to_resume=resume_download)
    #  download
    for zip_file_name in files:
        download_file(zip_file_name, download_dir, year)


def rm_sec(date: datetime) -> datetime:
    return date.replace(second=0, tzinfo=None)


def subsample_file(file_name, download_dir, filtered_dir, min_time_interval) -> str:
    logging.info("Subsampling  %s " % str(file_name))
    header = True

    try:
        for df_chunk in pd.read_csv(Path(download_dir, file_name), chunksize=CHUNK_SIZE):
            df_chunk = df_chunk.drop(['Unnamed: 0', 'MMSI', 'VesselName', 'CallSign', 'Cargo', 'TranscieverClass',
                                      'ReceiverType', 'ReceiverID'], axis=1, errors='ignore')
            df_chunk = df_chunk.dropna()
            df_chunk['SOG'] = pd.to_numeric(df_chunk['SOG'])
            if 'VesselType' in df_chunk.columns:
                df_chunk = df_chunk.query(
                    '(Status == "under way using engine" or Status == "under way sailing" or  Status == 8 or  Status == 0 or (SOG > 7 &  Status == "undefind")) & (VesselType == 1003 or VesselType == 1004 or VesselType == 1016 or 89 >= VesselType >= 70) & SOG > 3')
            elif 'Status' in df_chunk.columns:
                df_chunk = df_chunk.query(
                    '(Status == "under way using engine" or Status == "under way sailing" or  Status == 8 or  Status == 0) & SOG > 3')

            df_chunk = df_chunk.drop(['Status'], axis=1, errors='ignore')

            # parse and set seconds to zero
            df_chunk['BaseDateTime'] = pd.to_datetime(
                df_chunk.BaseDateTime, format='%Y-%m-%dT%H:%M:%S', exact=True, errors='raise').apply(rm_sec)
            df_chunk.index = df_chunk.BaseDateTime
            df_chunk = df_chunk.resample("%dT" % int(min_time_interval)).last()
            df_chunk.reset_index(drop=True, inplace=True)
            df_chunk = df_chunk.dropna()
            file_path = Path(filtered_dir, str(file_name))
            df_chunk.to_csv(file_path, mode='a', header=header, index=False , date_format='%Y-%m-%d %H:%M:%S')
            header = False
    except Exception as e:
        # discard the file in case of an error to resume later properly
        if file_path:
            file_path.unlink(missing_ok=True)
        raise FileFailedException(str(file_name), e)


def subsample_year_AIS_to_CSV(year: int, download_dir: Path, filtered_dir: Path, min_time_interval: int = 30) -> None:
    logger.info('Subsampling year {0} to {1} minutes.'.format(
        year, min_time_interval))
    # check already processed files in the
    resume = check_dir(filtered_dir) + Failed_Files

    files = [f for f in sorted(os.listdir(str(download_dir)), key=str.lower) if f.endswith('.csv') and f not in resume]
    for file in files:
        subsample_file(file, download_dir, filtered_dir, min_time_interval)
