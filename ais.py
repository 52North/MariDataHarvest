import logging

import wget
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import zipfile
from pathlib import Path
from check_connection import CheckConnection

pd.options.mode.chained_assignment = None
import sys
import warnings
import time

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


def check_dir(dir_name):
    history = []
    for path, dirs, files in os.walk(dir_name):
        for file in files:
            if file.endswith('.csv'):
                history.append(file)
    return history


def download_AIS(year):
    # create a directory named after the given year if not exist
    Path(str(year)).mkdir(parents=True, exist_ok=True)

    # check already installed files in the
    resume_download = check_dir(str(year))

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
            name, _ = a['href'].split('.')
            if name + '.csv' in resume_download: continue
            files.append(a['href'])

    #  download
    for file in files:
        CheckConnection.is_online()
        # create output name and directory
        output = os.path.join(str(year), '%s_%s' % (year, file.split('.')[0]))
        Path(output).mkdir(parents=True, exist_ok=True)
        logger.debug('downloading ais files {0}'.format(file))

        # download zip file using wget with url and file name
        wget.download(os.path.join(url, file), bar = None)

        # extract each zip file into output directory then delete it
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(output)
        os.remove(file)


def rm_sec(date):
    return date.replace(second=0)


def subsample_AIS_to_CSV(year, min_time_interval=30):
    # create a directory named after the given year if not exist
    logger.info('Subsampling year {0} to {1} minutes.'.format(year, min_time_interval))
    output = '{0}_filtered_{1}'.format(year, min_time_interval)
    Path(output).mkdir(parents=True, exist_ok=True)

    # check already processed files in the
    resume = check_dir(output)
    last_index = len(resume)
    for path, dirs, files in os.walk(str(year), followlinks=True):
        for file in [file for file in files if file.lower().endswith('.csv') and file not in resume]:
            last_index += 1
            logger.info("Subsampling {0}/{1} ".format(str(path), file))
            df = pd.read_csv(Path(path, file))
            df = df.drop(['MMSI', 'VesselName', 'CallSign', 'Cargo', 'TranscieverClass'], axis=1, errors='ignore')
            df = df.dropna()
            df = df.query(
                '(Status == "under way using engine" or Status == "under way sailing" or  Status == 8 or  Status == 0) & (VesselType == 1016 or 89 >= VesselType >= 70) & SOG > 3 & Length > 3 & Width > 3 & Draft > 3 ')
            df = df.drop(['Status'], axis=1, errors='ignore')
            # parse and set seconds to zero
            df['BaseDateTime'] = pd.to_datetime(df.BaseDateTime, format='%Y-%m-%dT%H:%M:%S').apply(rm_sec)
            df.index = df.BaseDateTime
            df = df.resample("%dT" % int(min_time_interval)).last()
            df.reset_index(drop=True, inplace=True)
            df.to_csv(Path(output, str(file)))
