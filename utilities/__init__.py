#   Copyright (C) 2021 - 2023 52°North Spatial Information Research GmbH
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# If the program is linked with libraries which are licensed under one of
# the following licenses, the combination of the program with the linked
# library is not considered a "derivative work" of the program:
#
#     - Apache License, version 2.0
#     - Apache Software License, version 1.0
#     - GNU Lesser General Public License, version 3
#     - Mozilla Public License, versions 1.0, 1.1 and 2.0
#     - Common Development and Distribution License (CDDL), version 1.0
#
# Therefore the distribution of the program linked with libraries licensed
# under the aforementioned licenses, is permitted by the copyright holders
# if the distribution is compliant with both the GNU General Public
# License version 2 and the aforementioned licenses.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
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
