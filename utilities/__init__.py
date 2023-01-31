import logging.config
import os
from pathlib import Path

import yaml

logging_config_file = Path(Path(__file__).parent, 'logging.yaml')

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
