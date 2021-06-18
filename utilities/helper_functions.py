from datetime import datetime
from pathlib import Path
import typing
import os

import pandas as pd

# string dates converters
str_to_date = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
date_to_str = lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ')
str_to_date_min = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M')

# Chunk size to manage huge files
CHUNK_SIZE = 10000


# Custom exception to retrieve file names with exception handling
class FileFailedException(Exception):
    def __init__(self, file_name, original_exception: Exception):
        self.file_name = file_name
        self.exceptionType = type(original_exception).__name__
        self.original_exception = original_exception
        super().__init__('Failed processing file %s' % file_name)


def SaveToFailedList(file_name, reason, work_dir):
    pd.DataFrame([[datetime.now().strftime("%Y-%m-%d %H:%M:%S"), file_name, reason]]).to_csv(
        Path(work_dir, 'FailedFilesList.csv'), mode='a', index=False, header=False)


# a list that contains all failed files with the corresponding reason
Failed_Files = []


def init_Failed_list(arg_string, work_dir):
    pd.DataFrame([[datetime.now().strftime("%Y-%m-%d %H:%M:%S"), arg_string, '']],
                 columns=['Timestamp', 'file_name', 'reason']).to_csv(
        Path(work_dir, 'FailedFilesList.csv'), index=False)


def check_dir(dir_name: Path) -> typing.List[str]:
    """
        List all contents of `dir_name` and returns is sorted using `str.lower` for `sorted`.
    """
    return sorted(os.listdir(dir_name), key=str.lower)


def create_csv(df, metadata_dict, file_path, index=True):
    """
         create a csv file including a metadata object as a dictionary in the beginning of the file.
     """
    csv_str = df.to_csv(index=index)
    csv_coma_line = csv_str[:csv_str.find('\n')].count(',') * ',' + '\n'
    csv_str = csv_coma_line.join(metadata_dict.values()) + csv_coma_line + csv_str
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        f.write(csv_str)
