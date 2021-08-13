import argparse
import logging
import os
import shutil
import time
import traceback
from pathlib import Path
from ais import download_year_AIS, subsample_year_AIS_to_CSV, download_file, get_files_list, subsample_file
from utilities.helper_functions import Failed_Files, SaveToFailedList, init_Failed_list, FileFailedException, check_dir
from EnvironmentalData.weather import append_to_csv

logger = logging.getLogger(__name__)


def years_arg_parser(input: str) -> list[int]:
    years = input.split('-')
    choices = list(range(2009, 2021))
    if len(years) == 2:
        start = years[0]
        end = years[1]
        try:
            if int(start) in choices and int(end) in choices:
                if start < end:
                    return list(range(int(start), int(end) + 1))
                elif start == end:
                    return [start]
            raise ValueError
        except Exception:
            raise argparse.ArgumentTypeError(
                "'" + input + "' is not Valid. Expected input 'YYYY' , 'YYYY-YYYY' or 'YYYY,YYYY,YYYY'.")

    years = input.split(',')
    if len(years) > 1:
        try:
            parsed_years = [int(y) for y in years if int(y) in choices]
            if len(parsed_years) < len(years):
                raise ValueError
            return parsed_years
        except ValueError as e:
            raise argparse.ArgumentTypeError(
                "'" + input + "' is not Valid. Expected input 'YYYY' , 'YYYY-YYYY' or 'YYYY,YYYY,YYYY'.")

    if len(years) == 1:
        try:
            parsed_y = int(input)
            if not parsed_y in choices:
                raise ValueError
            return [parsed_y]
        except ValueError as e:
            raise argparse.ArgumentTypeError(
                "'" + input + "' is not Valid. Expected input 'YYYY' , 'YYYY-YYYY' or 'YYYY,YYYY,YYYY'.")


def init_directories(dir, year, minutes):
    download_dir = Path(dir, str(year))
    merged_dir = Path(dir, str(year) + '_merged_%s' % minutes)
    filtered_dir = Path(dir, '{0}_filtered_{1}'.format(str(year), minutes))
    download_dir.mkdir(parents=True, exist_ok=True)
    merged_dir.mkdir(parents=True, exist_ok=True)
    filtered_dir.mkdir(parents=True, exist_ok=True)
    return download_dir, filtered_dir, merged_dir


if __name__ == '__main__':
    # arguments parameters
    parser = argparse.ArgumentParser(
        description='For a given a year and minutes interval of subsampling to start harvesting AIS-Data.',
        epilog='The following exit codes are configured:\n16 -> service secrets configuration file not found.')
    parser.add_argument('-y', '--year',
                        help="A given year to start a task. Expected input 'YYYY' , 'YYYY-YYYY' or 'YYYY,YYYY,YYYY'",
                        required=True, type=years_arg_parser)
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
    arg_string = 'Starting a task for year(s) %s with subsampling of %d minutes' % (
        ','.join(list(map(str, args.year))).join(['[', ']']), int(args.minutes))

    logger.info( arg_string + '. The output files will be saved to %s' % (args.dir if args.dir != '' else 'project directory'))
    args.dir = Path().absolute().parent if args.dir == '' else Path(args.dir)
    init_Failed_list(arg_string, args.dir)
    for year in args.year:
        logger.info('Processing year %s' % str(year))
        # initialize directories
        download_dir, filtered_dir, merged_dir = init_directories(args.dir, year, args.minutes)
        merged_dir_list = check_dir(merged_dir)
        filtered_dir_list = check_dir(filtered_dir)
        download_dir_list = check_dir(download_dir)
        if args.depth_first:
            logger.info('Task is started using Depth-first mode')
            for file in get_files_list(year, exclude_to_resume=merged_dir_list):
                file_name = file.split('.')[0] + '.csv'
                file_failed = False
                interval = 10
                while True:
                    try:
                        if (args.step == 1 or not file_name in filtered_dir_list) and not file_name in download_dir_list:
                            logger.info('STEP 1/3 downloading AIS data: %s' % file)
                            file_name = download_file(file, download_dir, year)
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when downloading AIS data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning('Skipping steps 1, 2 and 3 for file %s after attempting %d times' % (
                                file, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            interval = 10
                            file_failed = True
                            break
                        logger.error('Re-run in {0} sec'.format(interval))
                        time.sleep(interval)
                        interval += 10
                if args.step == 1:
                    continue
                while True:
                    try:
                        if file_failed: break
                        if file_name in filtered_dir_list:
                            logger.info(
                                'STEP 2/3 File: %s has been already subsampled from a previous run.' % file_name)
                            break
                        logger.info('STEP 2/3 subsampling CSV data: %s' % file_name)
                        subsample_file(file_name, download_dir, filtered_dir, args.minutes)
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when subsampling CSV data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning(
                                'Skipping steps 2, 3 for file %s after attempting %d times' % (file, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            interval = 10
                            file_failed = True
                            break
                        logger.error('Re-run in {0} sec'.format(interval))
                        time.sleep(interval)
                        interval += 10

                if args.clear and not file_failed:
                    logger.info('Remove raw file %s' % file_name)
                    if Path(download_dir, file_name).exists():
                        os.remove(str(Path(download_dir, file_name)))
                    else:
                        logger.warning("File not found  %s " % str(Path(download_dir, file_name)))

                while True:
                    try:
                        if file_failed: break
                        logger.info('STEP 3/3 appending weather data: %s' % file_name)
                        append_to_csv(Path(filtered_dir, file_name), Path(merged_dir, file_name))
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when appending environment data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning(
                                'Skipping step 3 for file %s after attempting %d times' % (file, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            break
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
                        download_year_AIS(year, download_dir)
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when downloading AIS data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning(
                                'Skipping step 1 for file %s after attempting %d times' % (e.file_name, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            interval = 10
                        logger.error('Re-run in {0} sec'.format(interval))
                        time.sleep(interval)
                        interval += 10

            if args.step == 0 or args.step == 2:
                # subset and filter data
                interval = 10
                while True:
                    try:
                        logger.info('STEP 2/3 subsampling CSV data')
                        subsample_year_AIS_to_CSV(str(year), download_dir, filtered_dir, args.minutes)
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when subsampling CSV data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning('Skipping file step 2 for file %s after attempting %d times' % (
                                e.file_name, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            interval = 10
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
                        for file in filtered_dir_list:
                            if Path(merged_dir, file).exists() or file in Failed_Files: continue
                            append_to_csv(Path(filtered_dir, file), Path(merged_dir, file))
                        break
                    except FileFailedException as e:
                        logger.error(traceback.format_exc())
                        logger.error('Error when appending environment data')
                        if interval > 40:
                            Failed_Files.append(e.file_name)
                            logger.warning(
                                'Skipping step 3 for file %s after attempting %d times' % (e.file_name, interval // 10))
                            SaveToFailedList(e.file_name, e.exceptionType, args.dir)
                            interval = 10
                        logger.error('Re-run in {0} sec'.format(interval))
                        time.sleep(interval)
                        interval += 10
