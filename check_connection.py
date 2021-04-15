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
