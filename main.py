import time
import traceback
import logging
import logging.config
from ais import download_AIS, subsample_AIS_to_CSV
from check_connection import CheckConnection
from weather import append_environment_data
import argparse
import os
import yaml


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
    parser.add_argument('--year',    help='A given year to start a task.',
                        required=True,  type=int, choices=range(2009, int(time.strftime("%Y"))))
    parser.add_argument('--minutes', help='A given minutes interval to downscale the data.',
                        required=True,  type=int, choices=range(1, 1440))
    parser.add_argument('--step',    help='Select the specific step to perform.',
                        required=False, type=int, choices=range(0, 4), default=0)
    parser.add_argument('--dir',
                        help='The output directory to collect csv files. By default the root directory is used.',
                        default='',type=str , required=False)
    args = parser.parse_args()

    # initialize a Thread to check connection
    connectionChecker = CheckConnection(check_interval=8)
    connectionChecker.daemon = True
    connectionChecker.start()

    logger.info('Starting a task for year %s with subsampling of %d minutes. The output files will be saved to %s' % (
            str(args.year), int(args.minutes), args.dir if args.dir != '' else 'project directory'))
    interval = 10
    if args.step != 0:
        logger.info('Single step selected')
    if args.step == 0 or args.step == 1:
        while True:
            try:
                logger.info('STEP 1/3 downloading AIS data')
                # download AIS data
                download_AIS(args.year, args.dir)
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
                subsample_AIS_to_CSV(str(args.year), args.dir, args.minutes)
                break
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('Error when subsampling CSV data')
                logger.error('Re-run in {0} sec'.format(interval))
                time.sleep(interval)
            interval += 10

    if args.step == 0 or args.step == 3:
        # append weather data for each row in the filtered data
        interval = 10
        while True:
            try:
                logger.info('STEP 3/3 appending weather data')
                append_environment_data(str(args.year), args.minutes, args.dir)
                break
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('Error when appending environment data')
                logger.error('Re-run in {0} sec'.format(interval))
                time.sleep(interval)
                interval += 10
