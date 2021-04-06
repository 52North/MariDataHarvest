
# Custom exception to retrieve file names with exception handling
class FileFailedException(Exception):
    def __init__(self, file_name):
        self.file_name = file_name
        super().__init__('Failed processing file %s' % file_name)


# a dictionary that contains all failed files with the corresponding reason
Failed_Files = dict()