import warnings
import logging
import os
import shutil
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup

from check_connection import CheckConnection
from config import config
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


def check_dir(dir_name):
    return sorted(os.listdir(dir_name), key=str.lower)


def download_AIS(year, work_dir):
    # create a directory named after the given year if not exist
    p = Path(work_dir, str(year))
    p.mkdir(parents=True, exist_ok=True)

    # check already installed files in the
    resume_download = check_dir(p)
    CheckConnection.set_url('coast.noaa.gov')
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
            if name + '.csv' in resume_download or name + '.gdb' in resume_download:
                continue
            files.append(a['href'])

    #  download
    for file in files:
        CheckConnection.is_online()
        logger.info('downloading AIS file: %s' % file)
        # download zip file using wget with url and file name
        with requests.get(os.path.join(url, file), stream=True) as req:
            req.raise_for_status()
            file = file.split('/')[-1] if len(file.split('/')) > 1 else file
            with open(file, "wb") as handle:
                for chunk in req.iter_content(chunk_size=8192):
                    handle.write(chunk)
                handle.close()
        # extract each zip file into output directory then delete it
        with zipfile.ZipFile(file, 'r') as zip_ref:
            for f in zip_ref.infolist():
                if f.filename.endswith('.csv'):
                    f.filename = os.path.basename(f.filename)
                    zip_ref.extract(f, p)
                if str(Path(f.filename).parent).endswith('.gdb'):
                    zip_ref.extractall(p)
                    name = str(Path(f.filename).parent)
                    gdb_file = Path(p, name)
                    gdf = gpd.read_file(gdb_file)
                    name = name.split('.')[0]
                    gdf['LON'] = gdf.geometry.apply(lambda point: point.x)
                    gdf['LAT'] = gdf.geometry.apply(lambda point: point.y)
                    gdf.drop(columns=['geometry'], inplace=True)
                    gdf.to_csv(Path(p, name + '.csv'))
                    shutil.rmtree(gdb_file)
                    break
        os.remove(file)


def rm_sec(date):
    return date.replace(second=0)


def subsample_AIS_to_CSV(year, work_dir, min_time_interval=30):
    # create a directory named after the given year if not exist
    output = '{0}_filtered_{1}'.format(year, min_time_interval)
    path = Path(work_dir, output)
    Path(work_dir, output).mkdir(parents=True, exist_ok=True)
    logger.info('Subsampling year {0} to {1} minutes.'.format(
        year, min_time_interval))
    # check already processed files in the
    resume = check_dir(Path(work_dir, output))
    chunkSize = 500000
    files = [f for f in sorted(os.listdir(str(
        Path(work_dir, year))), key=str.lower) if f.endswith('.csv') and f not in resume]
    for file in files:
        logging.info("Subsampling  %s " % str(file))
        header = True
        for df_chunk in pd.read_csv(Path(Path(work_dir, year), file), chunksize=chunkSize):
            df_chunk = df_chunk.drop(['MMSI', 'VesselName', 'CallSign', 'Cargo', 'TranscieverClass',
                          'ReceiverType', 'ReceiverID'], axis=1, errors='ignore')
            df_chunk = df_chunk.dropna()
            if 'VesselType' in df_chunk.columns:
                df_chunk = df_chunk.query(
                    '(Status == "under way using engine" or Status == "under way sailing" or  Status == 8 or  Status == 0) & (VesselType == 1016 or 89 >= VesselType >= 70) & SOG > 3')
            else:
                df_chunk = df_chunk.query(
                    '(Status == "under way using engine" or Status == "under way sailing" or  Status == 8 or  Status == 0) & SOG > 3')
            df_chunk = df_chunk.drop(['Status'], axis=1, errors='ignore')
            # parse and set seconds to zero
            df_chunk['BaseDateTime'] = pd.to_datetime(
                df_chunk.BaseDateTime, format='%Y-%m-%dT%H:%M:%S').apply(rm_sec)
            df_chunk.index = df_chunk.BaseDateTime
            df_chunk = df_chunk.resample("%dT" % int(min_time_interval)).last()
            df_chunk.reset_index(drop=True, inplace=True)
            df_chunk = df_chunk.dropna()
            df_chunk.to_csv(Path(path, str(file)), chunksize=chunkSize, mode='a', header=header)
            header = False
