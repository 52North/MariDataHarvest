import logging
import time
from threading import Thread
import http.client as httplib


class CheckConnection(Thread):
    online = True
    domain_url = ''
    def __init__(self, check_interval):
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
                        logging.warning('Internet Connection is established to server %s' % CheckConnection.domain_url)
                    CheckConnection.online = True
                except Exception as e:
                    CheckConnection.online = False
                    logging.error(e)
                    logging.warning('No Internet Connection to server %s' % CheckConnection.domain_url)
                finally:
                    conn.close()
                time.sleep(self.check_interval)

    @staticmethod
    def is_online():
        if CheckConnection.online:
            pass
        else:
            raise ValueError('No Internet Connection')

    @staticmethod
    def set_url(domain):
        CheckConnection.domain_url = domain
