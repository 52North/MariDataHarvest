import argparse
import logging
import logging.config
import os
import shutil
import time
import traceback
from pathlib import Path

import yaml

from ais import download_year_AIS, subsample_year_AIS_to_CSV, download_file, get_files_list, subsample_file, check_dir
from check_connection import CheckConnection
from weather import append_environment_data_to_year, append_environment_data_to_file

logging_config_file = 'logging.yaml'
level = logging.DEBUG

if os.path.exists(logging_config_file):
    with open(logging_config_file, 'rt') as file:
        try:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
        except Exception as e:
            print(e)
            print(
                'Error while loading logging configuration from file "%s". Using defaults' % logging_config_file)
            logging.basicConfig(level=level)
else:
    print('Logging file configuration does not exist: "%s". Using defaults.' %
          logging_config_file)
    logging.basicConfig(level=level)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    # arguments parameters
    parser = argparse.ArgumentParser(
        description='For a given a year and minutes interval of subsampling to start harvesting AIS-Data.',
        epilog='The following exit codes are configured:\n16 -> service secrets configuration file not found.')
    parser.add_argument('-y', '--year', help='A given year to start a task.',
                        required=True, type=int, choices=range(2009, int(time.strftime("%Y"))))
    parser.add_argument('-m', '--minutes', help='A given minutes interval to downscale the data.',
                        required=True, type=int, choices=range(1, 1440))
    parser.add_argument('-s', '--step', help='Select the specific step to perform.',
                        required=False, type=int, choices=range(0, 4), default=0)
    parser.add_argument('-d', '--dir',
                        help='The output directory to collect csv files. By default the root directory is used.',
                        default='', type=str, required=False)
    parser.add_argument('-c', '--clear',
                        help='Clears the raw output directory in order to free memory.',
                        action='store_true')
    parser.add_argument('-f', '--depth-first',
                        help='Clears the raw output directory in order to free memory.',
                        action='store_true')
    args, unknown = parser.parse_known_args()

    # initialize a Thread to check connection
    connectionChecker = CheckConnection(check_interval=8)
    connectionChecker.daemon = True
    connectionChecker.start()

    logger.info('Starting a task for year %s with subsampling of %d minutes. The output files will be saved to %s' % (
        str(args.year), int(args.minutes), args.dir if args.dir != '' else 'project directory'))

    # initialize directories
    download_dir = Path(args.dir, str(args.year))
    merged_dir = Path(args.dir, str(args.year) + '_merged_%s' % args.minutes)
    filtered_dir = Path(args.dir, '{0}_filtered_{1}'.format(str(args.year), args.minutes))
    download_dir.mkdir(parents=True, exist_ok=True)
    merged_dir.mkdir(parents=True, exist_ok=True)
    filtered_dir.mkdir(parents=True, exist_ok=True)

    interval = 10
    if args.depth_first:
        logger.info('Task is started using Depth-first mode')

        for file in get_files_list(args.year, check_dir(merged_dir)):
            while True:
                try:
                    logger.info('STEP 1/3 downloading AIS data: %s' % file)
                    file_name = download_file(file, download_dir, args.year)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when downloading AIS data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                    interval += 10

            while True:
                try:
                    logger.info('STEP 2/3 subsampling CSV data: %s' % file_name)
                    subsample_file(file_name, download_dir, filtered_dir, args.minutes)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when subsampling CSV data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                    interval += 10

            if args.clear:
                logger.info('Remove raw file %s' % file_name)
                if Path(download_dir, file_name).exists():
                    os.remove(str(Path(download_dir, file_name)))
                else:
                    logger.error("Error: %s file not found" % str(Path(download_dir, file_name)))

            while True:
                try:
                    logger.info('STEP 3/3 appending weather data: %s' % file_name)
                    append_environment_data_to_file(file_name, filtered_dir, merged_dir)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when appending environment data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                    interval += 10
    else:
        if args.step != 0:
            logger.info('Single step selected')
        if args.step == 0 or args.step == 1:
            while True:
                try:
                    logger.info('STEP 1/3 downloading AIS data')
                    # download AIS data
                    download_year_AIS(args.year, download_dir)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when downloading AIS data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                    interval += 10

        if args.step == 0 or args.step == 2:
            # subset and filter data
            interval = 10
            while True:
                try:
                    logger.info('STEP 2/3 subsampling CSV data')
                    subsample_year_AIS_to_CSV(str(args.year), download_dir, filtered_dir, args.minutes)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when subsampling CSV data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                interval += 10
            if args.clear:
                logger.info('Remove raw files and clear directory of year %s  ' % str(download_dir))
                if download_dir.exists():
                    shutil.rmtree(download_dir)

        if args.step == 0 or args.step == 3:
            # append weather data for each row in the filtered data
            interval = 10
            while True:
                try:
                    logger.info('STEP 3/3 appending weather data')
                    append_environment_data_to_year(filtered_dir, merged_dir)
                    break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('Error when appending environment data')
                    logger.error('Re-run in {0} sec'.format(interval))
                    time.sleep(interval)
                    interval += 10


