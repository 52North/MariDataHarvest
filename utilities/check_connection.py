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
import logging
import time
from threading import Thread
import http.client as httplib

logger = logging.getLogger(__name__)
class CheckConnection(Thread):
    online = True
    domain_url = ''
    check_interval = 30

    def __init__(self, check_interval: int) -> None:
        super().__init__()
        self.check_interval = check_interval

    def run(self):
        while True:
            if CheckConnection.domain_url:
                conn = httplib.HTTPConnection(CheckConnection.domain_url, timeout=30)
                try:
                    # check connection
                    conn.request("HEAD", "/")
                    if not CheckConnection.online:
                        logger.warning('Internet Connection is established to server %s' % CheckConnection.domain_url)
                    CheckConnection.online = True
                except Exception as e:
                    CheckConnection.online = False
                    logger.error(e)
                    logger.warning('No Internet Connection to server %s' % CheckConnection.domain_url)
                finally:
                    conn.close()
                time.sleep(self.check_interval)

    @staticmethod
    def is_online() -> None:
        if CheckConnection.online:
            pass
        else:
            raise ValueError('No Internet Connection')

    @staticmethod
    def set_url(domain: str) -> None:
        CheckConnection.domain_url = domain
