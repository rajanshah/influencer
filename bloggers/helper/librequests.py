# -*- coding: utf-8 -*-

import cookielib
import requests
import time
import urlparse
from datetime import datetime

DEFAULT_DELAY = 5

class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urlparse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - \
                                       last_accessed).seconds
            if sleep_secs > 0:
                # domain has been accessed recently
                # so need to sleep
                time.sleep(sleep_secs)
        # update the last accessed time
        self.domains[domain] = datetime.now()

# request session
s = requests.session()
s.headers.update({
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 \
            Safari/537.36'
    })
# throttle for delay
throttle = Throttle(DEFAULT_DELAY)

def is_site_up(url):
    response = s.get(url)
    return response.ok

def get_source(url):
    throttle.wait(url)    
    response = s.get(url)
    return response.text

def post_source(url, data):
    throttle.wait(url)
    response = s.post(url, data)
    return response.text

