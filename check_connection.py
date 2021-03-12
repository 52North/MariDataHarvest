import time
from threading import Thread
import http.client as httplib


class CheckConnection(Thread):
    online = True

    def __init__(self, check_interval):
        super().__init__()
        self.check_interval = check_interval

    def run(self):
        while True:
            conn = httplib.HTTPConnection('google.com', timeout=2)
            try:
                # check connection
                conn.request("HEAD", "/")
                if not CheckConnection.online:
                    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), 'Internet Connection is established')
                CheckConnection.online = True
            except Exception as e:
                CheckConnection.online = False
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),'No Internet Connection')
            finally:
                conn.close()
            time.sleep(self.check_interval)

    @staticmethod
    def is_online():
        if CheckConnection.online:
            pass
        else:
            raise ValueError('No Internet Connection')
