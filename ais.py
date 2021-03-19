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
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def check_dir(dir_name):
    history = []
    # for path, dirs, files in os.walk(dir_name):
    #     for file in files:
    #         if file.endswith('.csv'):
    #             history.append(file)
    #         if file.endswith('gdb'):
    #             history.append(file)

    return os.listdir(dir_name)


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
            name, _ = a['href'].split('.')
            name = name.split('/')[-1] if len(name.split('/')) > 1 else name
            if name + '.csv' in resume_download or name + '.gdb' in resume_download: continue
            files.append(a['href'])

    #  download
    for file in files:
        CheckConnection.is_online()
        # create output name and directory
        logging.info('downloading AIS file: %s' % file)
        # download zip file using wget with url and file name
        wget.download(os.path.join(url, file))
        file = file.split('/')[-1] if len(file.split('/')) > 1 else file
        # extract each zip file into output directory then delete it
        with zipfile.ZipFile(file, 'r') as zip_ref:
            for f in zip_ref.namelist():
                if f.endswith('.csv'):
                    zip_ref.extract(f, p)
                if f.endswith('.gdb'):
                    zip_ref.extractall(p)
        os.remove(file)


def rm_sec(date):
    return date.replace(second=0)


def subsample_AIS_to_CSV(year, work_dir, min_time_interval=30):
    # create a directory named after the given year if not exist
    output = str(year) + '_filtered_%s' % min_time_interval
    path = Path(work_dir, output)
    Path(work_dir, output).mkdir(parents=True, exist_ok=True)

    # check already processed files in the
    resume = check_dir(Path(work_dir, output))
    last_index = len(resume)
    for file in os.listdir(str(Path(work_dir, year))):
        if file.endswith('.csv') and file not in resume:
            last_index += 1
            logging.info("Subsampling  %s " % str(file))
            df = pd.read_csv(Path(Path(work_dir, year), file))
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
            df.to_csv(Path(path, str(file)))
