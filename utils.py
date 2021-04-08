import datetime
from pathlib import Path

import pandas as pd


# Custom exception to retrieve file names with exception handling
class FileFailedException(Exception):
    def __init__(self, file_name, original_exception: Exception):
        self.file_name = file_name
        self.exceptionType = type(original_exception).__name__
        super().__init__('Failed processing file %s' % file_name)


def SaveToFailedList(file_name, reason, work_dir):
    pd.DataFrame([[datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), file_name, reason]]).to_csv(
        Path(work_dir, 'FailedFilesList.csv'), mode='a', index=False, header=False)


# a list that contains all failed files with the corresponding reason
Failed_Files = []


def init_Failed_list(arg_string, work_dir):
    pd.DataFrame([[datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), arg_string, '']],
                 columns=['Timestamp', 'file_name', 'reason']).to_csv(
        Path(work_dir, 'FailedFilesList.csv'), index=False)
