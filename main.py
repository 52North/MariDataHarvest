import time
import traceback
from ais import download_AIS, subsample_AIS_to_CSV
from check_connection import CheckConnection
from weather import append_environment_data
import argparse
import logging

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='For a given a year and minutes interval of subsampling to start harvesting AIS-Data.')
    parser.add_argument('--year', help='A given year to start a task.', required=True)
    parser.add_argument('--minutes', help='A given minutes interval to downscale the data.', required=True)
    args = parser.parse_args()
    logging.getLogger().handlers = []
    logging.basicConfig(filename='maridataharvest.log',
                        level=logging.NOTSET,
                        format='%(asctime)s %(message)s',
                        datefmt='%d.%m.%Y %H:%M:%S')
    if len(str(args.year)) == 4:
        connectionChecker = CheckConnection(check_interval=8)
        connectionChecker.daemon = True
        connectionChecker.start()

        logging.info('Starting a task for year %s' % str(args.year))
        interval = 10
        while True:
            try:
                logging.debug('STEP 1/3 downloading AIS data')
                # download AIS data
                download_AIS(args.year)
                break
            except Exception as e:
                logging.error(traceback.format_exc())
                logging.warning('Error when downloading AIS data')
                logging.warning('Re-run in %s sec....' % interval)
                time.sleep(interval)
                interval += 10

        # subset and filter data
        interval = 10
        while True:
            try:
                logging.debug('STEP 2/3 subsampling CSV data')
                subsample_AIS_to_CSV(args.year, min_time_interval=args.minutes)
                break
            except Exception as e:
                logging.error(traceback.format_exc())
                logging.warning('Error when subsampling CSV data')
                logging.warning('Re-run in %s sec....' % interval)
                time.sleep(interval)
                interval += 10

        # append weather data for each row in the filtered data
        interval = 10
        while True:
            try:
                logging.debug('STEP 3/3 appending weather data')
                append_environment_data(args.year, min_time_interval=args.minutes)
                break
            except Exception as e:
                logging.error(traceback.format_exc())
                logging.warning('Error when appending environment data ....')
                logging.warning('Re-run in %s sec....' % interval)
                time.sleep(interval)
                interval += 10
    else:
        logging.error('please enter the year parameter as YYYY')
