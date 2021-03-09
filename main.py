import time
import traceback
from ais import download_AIS, subsample_AIS_to_CSV
from weather import append_environment_data
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='For a given a year and minutes interval of subsampling to start harvesting AIS-Data.')
    parser.add_argument('--year', help='A given year to start a task.', required=True)
    parser.add_argument('--minutes', help='A given minutes interval to downscale the data.', required=True)
    args = parser.parse_args()

    if len(str(args.year)) == 4:
        print('Starting a task for year %s .... \n' % str(args.year))
        interval = 10
        while True:
            try:
                print('  1/3 downloading AIS data \n')
                # download AIS data
                download_AIS(args.year)
                break
            except Exception as e:
                print(traceback.format_exc())
                print('    Error when downloading AIS data .... \n')
                print('\n\nRe-run in %s sec....\n\n' % interval)
                time.sleep(interval)
                interval += 10

        # subset and filter data
        interval = 10
        while True:
            try:
                print('\n  2/3 subsampling CSV data \n')
                subsample_AIS_to_CSV(args.year, min_time_interval=args.minutes)
                break
            except Exception as e:
                print(traceback.format_exc())
                print('    Error when subsampling CSV data .... \n')
                print('\n\nRe-run in %s sec....\n\n' % interval)
                time.sleep(interval)
                interval += 10

        # append weather data for each row in the filtered data
        interval = 10
        while True:
            try:
                print('\n  3/3 appending weather data .... \n')
                append_environment_data(args.year, min_time_interval=args.minutes)
                break
            except Exception as e:
                print(traceback.format_exc())
                print('    Error when appending environment data .... \n')
                print('\n\nRe-run in %s sec....\n\n' % interval)
                time.sleep(interval)
                interval += 10
    else:
        print('please enter the year parameter as YYYY')
