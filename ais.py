from datetime import datetime
import warnings
import logging
import os
import shutil
import zipfile
import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup

from check_connection import CheckConnection
from config import config
from utils import FileFailedException, Failed_Files

pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


def check_dir(dir_name: Path) -> typing.List[str]:
    """
        List all contents of `dir_name` and returns is sorted using `str.lower` for `sorted`.
    """
    return sorted(os.listdir(dir_name), key=str.lower)


def get_files_list(year: int, exclude_to_resume: typing.List[str]) -> typing.List[str]:
    # url link to data
    url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{0}/".format(year)
    # check already installed files in the
    CheckConnection.set_url('coast.noaa.gov')

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
            Failed_Files_list = list(Failed_Files.keys())
            if name + '.csv' in exclude_to_resume + Failed_Files_list or name + '.gdb' in exclude_to_resume + Failed_Files_list or name + '.zip' in Failed_Files_list:
                continue
            files.append(a['href'])
    return files


def chunkify_gdb(gdb_file: Path, file_path: Path, chunkSize: int) -> None:
    end = chunkSize
    start = 0
    header = True
    while True:
        gdf_chunk = gpd.read_file(gdb_file, rows=slice(start, end))
        if len(gdf_chunk) == 0: break
        gdf_chunk['LON'] = gdf_chunk.geometry.apply(lambda point: point.x)
        gdf_chunk['LAT'] = gdf_chunk.geometry.apply(lambda point: point.y)
        gdf_chunk.drop(columns=['geometry'], inplace=True)
        gdf_chunk.to_csv(file_path, mode='a', header=header)
        start = end
        end += chunkSize
        header = False


def download_file(zipped_file_name: str, download_dir: Path, year: int) -> str:
    try:
        # url link to data
        url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{0}/".format(year)
        CheckConnection.is_online()
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
                        chunkify_gdb(gdb_file, file_path, chunkSize=100000)
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
        raise FileFailedException(file_name=zipped_file_name)


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
    chunkSize = 100000
    logging.info("Subsampling  %s " % str(file_name))
    header = True

    try:
        for df_chunk in pd.read_csv(Path(download_dir, file_name), chunksize=chunkSize):
            df_chunk = df_chunk.drop(['MMSI', 'VesselName', 'CallSign', 'Cargo', 'TranscieverClass',
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
            df_chunk.to_csv(file_path, chunksize=chunkSize, mode='a', header=header)
            header = False
    except Exception as e:
        # discard the file in case of an error to resume later properly
        if file_path:
            file_path.unlink(missing_ok=True)
        raise FileFailedException(file_name=str(file_name))


def subsample_year_AIS_to_CSV(year: int, download_dir: Path, filtered_dir: Path, min_time_interval: int = 30) -> None:
    logger.info('Subsampling year {0} to {1} minutes.'.format(
        year, min_time_interval))
    # check already processed files in the
    resume = check_dir(filtered_dir) + list(Failed_Files.keys())

    files = [f for f in sorted(os.listdir(str(download_dir)), key=str.lower) if f.endswith('.csv') and f not in resume]
    for file in files:
        subsample_file(file, download_dir, filtered_dir, min_time_interval)
