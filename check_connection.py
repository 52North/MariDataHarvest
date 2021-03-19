import logging
import time
from threading import Thread
import http.client as httplib

logger = logging.getLogger(__name__)
class CheckConnection(Thread):
    online = True

    def __init__(self, check_interval):
        super().__init__()
        self.check_interval = check_interval

    def run(self):
        while True:
            conn = httplib.HTTPConnection('google.com', timeout=5)
            try:
                # check connection
                conn.request("HEAD", "/")
                if not CheckConnection.online:
                    logger.warning('Internet Connection is established')
                CheckConnection.online = True
            except Exception as e:
                CheckConnection.online = False
                logger.warning('No Internet Connection')
            finally:
                conn.close()
            time.sleep(self.check_interval)

    @staticmethod
    def is_online():
        if CheckConnection.online:
            pass
        else:
            raise ValueError('No Internet Connection')
