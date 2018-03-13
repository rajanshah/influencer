# -*- coding: utf-8 -*-

import cookielib
import logging
import mechanize
import random
import time
import urlparse
from datetime import datetime


class MechanizeBrowser(object):
    '''Mechanize Browser class that handles the mechanize features
    '''
    
    def __init__(self, delay, max_delay, min_delay):
        self.__setup_browser()
        self.throttle = Throttle(delay, max_delay, min_delay)

    def __setup_browser(self):
        '''Setup the mechanize browser with all the required features
        '''
        # setting up browser for login
        self.browser = mechanize.Browser()
        self.cookie_jar = cookielib.LWPCookieJar()
        self.browser.set_cookiejar(self.cookie_jar)

        # Browser options
        self.browser.set_handle_equiv(True)
        # self.browser.set_handle_gzip(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_handle_robots(False)
        self.browser.set_handle_refresh(
                mechanize._http.HTTPRefreshProcessor(), max_time=1)
        self.browser.addheaders = [('User-agent', 'Chrome')]
        self.login_required = False

    def reset(self):
        '''Reset feature for cases where the website has to reset to base url
        '''
        
        raise NotImplementedError("Base class should override " + \
                                    self.__class__.__name__ + '.reset')

    def open_url(self, url):
        '''Visits the url and returns the page source
        '''
        self.throttle.wait(url)
        response = self.browser.open(url).read()
        return response

    def get_response(self, url):
        '''Return the page source of the current visited url
        '''
        
        return self.browser.open(url)

    def set_login_required(self):
        '''Set the login flag for website that require authentication
        '''
        
        self.login_required = True

    def login(self):
        '''Login implementation left for derived class
        '''
        
        if self.login:
            raise NotImplementedError("Base class should override " + \
                                    self.__class__.__name__ + '.login')

class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay, max_delay, min_delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urlparse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay * random.randint(\
                 self.min_delay,self.max_delay) - \
                (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                # domain has been accessed recently
                # so need to sleep
                time.sleep(sleep_secs)
        # update the last accessed time
        self.domains[domain] = datetime.now()

