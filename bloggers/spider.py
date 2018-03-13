# -*- coding: utf-8 -*-


###################################################################################
#
# DATE:     01-06-2016
# AUTHOR:   AJAN LAL SHRESTHA
# PURPOSE:  Collect various ratings data
# LICENSE:  IntelliMind LLC
#
###################################################################################


import argparse
import hashlib
import json
import logging
import logging.handlers
import os
import time
import traceback
import urllib, urllib2, urlparse
from datetime import date

# from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

import parser
from xsutils import db, utility
from xsutils.jsonwriter import json_reader, JSONWriter
from xsutils.mechanizebrowser import MechanizeBrowser

LOG_FILENAME = '/tmp/intellimind.log'

my_logger = logging.getLogger('intellimind')
my_logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                               mode='a',
                                               )
my_logger.addHandler(handler)

class BloggerSpider(MechanizeBrowser):
    """
        Spider for scraping the seekingalpha author contents
    """

    def __init__(self, root_dir, config):
        self.root_dir = root_dir
        self.config = config
        delay = int(self.config.get('BLOGGERS','DEFAULT_DELAY'))
        max_delay = int(self.config.get('BLOGGERS','MAX_DELAY'))
        min_delay = int(self.config.get('BLOGGERS','MIN_DELAY'))
        MechanizeBrowser.__init__(self, delay, max_delay, min_delay)
        self.BASE_URL = self.config.get('BLOGGERS','BASE_URL')
        self.STARTING_URL = self.config.get('BLOGGERS','STARTING_URL')
        self.DAILY_STARTING_URL = self.config.get('BLOGGERS','DAILY_STARTING_URL')
        self.storage_folder = os.path.join(root_dir, self.config.get('BLOGGERS','STORAGE_FOLDER'))
        self.style_filename = self.config.get('BLOGGERS', \
                                              'STYLE_CATEGORY_FILENAME')
        self.cache_day = int(self.config.get('BLOGGERS','CACHE_DAY'))
        db_name = self.config.get('BLOGGERS', 'BLOGGER_DB')
        self.blogger_db = os.path.join(root_dir, db_name)
        # self.scheduler = BackgroundScheduler()
        self.scheduler = BlockingScheduler()
        self.SCHEDULE_INTERVAL = int(self.config.get('BLOGGERS','SCHEDULE_INTERVAL'))
        self.is_first_scrape = True

    @classmethod
    def get_today(self):
        '''Return today date'''
        return date.today()

    def save_to_json(self, root_dir, file_name, data):
        '''Save the data as json'''

        json_writer = JSONWriter(root_dir)
        json_writer.write_json_content(file_name, data)

    def get_blogger_id(self, url):
        return hashlib.sha224(url).hexdigest()

    def scrape_style_category_links(self):
        import ipdb
        ipdb.set_trace()
        source = self.open_url(self.STARTING_URL)
        links = parser.parse_links(source, self.BASE_URL)
        # store the links in json file
        self.save_to_json(self.storage_folder, self.style_filename, links)
        my_logger.debug("Stored style category to JSON")

    def author_links(self, category_page_url):
        author_links = []
        seen_links = set(category_page_url)
        next_link = ""
        try:
            while True:
                import ipdb
                ipdb.set_trace()
                source = self.open_url(category_page_url)
                author_links.extend(\
                            parser.parse_author_links(source, self.BASE_URL))
                next_link = parser.parse_next_category_link(source, self.BASE_URL)
                if next_link in seen_links:
                    break
                elif next_link:
                    seen_links.add(next_link)
                    category_page_url = next_link
                else:
                    break
        except Exception, e:
            my_logger.warn(str(e))
            my_logger.debug("Problem in extraction of categories and style")
            my_logger.debug("Main URL %s" % category_page_url)
            my_logger.debug("Next URL %s" % next_link)
        return author_links

    def scrape_author_links(self):
        try:
            links = json_reader(self.storage_folder, self.style_filename)
            for style in links.keys():
                categories = links[style]
                for category in categories:
                    folder = os.path.join(self.storage_folder,style)
                    file_name = '_'.join(category[0].split())
                    self.save_to_json(folder, file_name, self.author_links(
                                      category[-1]))
        except Exception, e:
            my_logger.warn(str(e))
            my_logger.debug("Problem in reading the category style from JSON \
                          file.")

    def author_info(self, author_url):
        try:
            source = self.open_url(author_url)
            author_info = parser.parse_author_info(source)
            return author_info
        except Exception, e:
            my_logger.warn(str(e))
            return {}

    def coverage_info(self, author_url, user_id):
        try:
            path = urlparse.urlsplit(author_url).path
            if "author" in path:
                url = author_url + "/ajax_load_articles_ticker_count?"
                data = dict(type="regular_articles",user_id=user_id)
            elif "user" in path:
                url = author_url + "/ajax_load_ticker_counts_per_activity_type?"
                data = dict(type="activity_feed",user_id=user_id)
            else:
                return {"ticker":json.dumps([])}
            url += urllib.urlencode(data)
            source = self.open_url(url)
            coverage_info = parser.parse_coverage_info(source)
            return coverage_info
        except Exception, e:
            my_logger.debug("Problem in extracting ticker")
            my_logger.warn(str(e))
            my_logger.debug("Author url: {}".format(author_url))
            my_logger.debug("User ID: {}".format(user_id))
            return {"ticker":json.dumps([])}

    def store_article_links(self, author, links):
        more = True
        if links:
            if len(links) >= 1:
                if db.article_exists(author, links[0]):
                    more = False
            if more:
                for link in links:
                    if db.article_exists(author, link):
                        more = False
                        break
                    db.save_article(author, link)
        else:
            more = False
        return more
        
    def scrape_article_links(self, author_url, author):
        page_no = 0
        links = []
        user_id = author.user_id
        try:
            path = urlparse.urlsplit(author_url).path
            if "author" in path:
                while True:
                    url = author_url + "/ajax_load_regular_articles?"
                    data = dict(page=page_no,author='true',userId=user_id,\
                                sort='recent')
                    url += urllib.urlencode(data)
                    source = self.open_url(url)
                    info = parser.parse_article_page(source)
                    link_info = parser.parse_html_article_links(\
                                    info['html_content'], self.BASE_URL)
                    links = link_info['article_url']
                    more = self.store_article_links(author, links)
                    page_no += 1
                    if not more or page_no == (info['page_count']):
                        break
            elif "user" in path:
                data = dict(user_id=user_id)
                while True:
                    url = author_url + "/ajax_load_activity_feed?"
                    url += urllib.urlencode(data)
                    source = self.open_url(url)
                    link_info = parser.parse_json_article_links(source, \
                                            self.BASE_URL)
                    links = link_info['article_url']
                    more = self.store_article_links(author, links)
                    time = link_info['time']
                    data['time'] = time
                    if not more or not time:
                        break
        except Exception, e:
            my_logger.debug("Problem in extracting articles")
            my_logger.warn(str(e))
            my_logger.debug("Author url: {}".format(author_url))
            my_logger.debug("User ID: {}".format(author.user_id))


    def encode_picture(self, pic_url):
        try:
            response = self.open_url(pic_url)
            encoded_data = utility.encode_base64(response)
            return encoded_data
        except urllib2.HTTPError, e:
            my_logger.warn(str(e))
            if e.code == 404:
                my_logger.debug("404 error")
            return None
        except Exception, e:
            my_logger.debug("Problem in encoding picture")
            my_logger.warn(str(e))
            my_logger.debug("Pic url: %s" % pic_url)
            my_logger.debug("Retrying")
            return self.encode_picture(pic_url)

    def get_final_url(self, url):
        try:
            response = self.get_response(url)
            if not url in response.geturl():
                url = response.geturl()[:response.geturl().rindex("/")]
            return url
        except urllib2.HTTPError, e:
            my_logger.warn(str(e))
            if e.code == 404:
                my_logger.debug("404 error")
            return ""
        except Exception, e:
            my_logger.debug("Problem in fetching final url")
            my_logger.warn(str(e))
            return ""

    def save_author_info(self, final_link, blogger_hash, author_name, style=None, category=None):
        author_info = self.author_info(final_link)
        if author_info:
            author_info['url'] = final_link
            author_info['blogger_hash'] = blogger_hash
            author_info['updated_date'] = date.today()
            user_id = author_info['user_id']

            author_info.update(self.coverage_info(final_link, \
                author_info['user_id']))

            author_info['picture'] = self.encode_picture(\
                                     author_info['picture'])
            
            author = db.save_author(author_info)
            if style and category:
                db.save_style_category(author, style, category)
        else:
            my_logger.debug("Problem in scraping author info")
            my_logger.debug("Author name: %s" % author_name)
            my_logger.debug("URL: %s" % final_link)

    def scrape_author_info(self):
        style_category = []
        links = json_reader(self.storage_folder, self.style_filename)
        for style in links.keys():
            categories = links[style]
            for category in categories:
                style_category.append("{}#{}".format(style, category[0]))
        try:
            logging.warn("Start")
            my_logger.debug(time.ctime())
            whole_start = time.time()
            for item in style_category:
                style, category = item.split("#")
                folder = os.path.join(self.storage_folder,style)
                file_name = '_'.join(category.split())
                author_links = json_reader(folder, file_name)
                category_start = time.time()
                for author_name, _link in author_links:#[:5]:
                    link = self.get_final_url(_link)
                    if not link:
                        continue
                    
                    my_logger.debug(item)
                    my_logger.debug(author_name)
                    my_logger.debug(link)
                    blogger_hash = self.get_blogger_id(link)
                    start = time.time()
                    author = None
                    if db.author_exists(blogger_hash) and \
                            db.within_cache(blogger_hash, self.cache_day):
                        author = db.duplicate(blogger_hash)
                        if not db.reentry(author, style, category):
                            db.save_style_category(author, style, category)
                        user_id = author.user_id
                    else:
                        self.save_author_info(link, blogger_hash, author_name, style, category)

                    if author:
                        self.scrape_article_links(link, author)
                    my_logger.debug("Time taken: {}\n".format(\
                                        utility.time_delta(start)))
                my_logger.debug("{} Time taken: {}\n".format(\
                            category, utility.time_delta(category_start)))
            my_logger.debug("Total Time taken: {}\n".format(\
                            utility.time_delta(whole_start)))
        except Exception, e:
            my_logger.warn(str(e))
        finally:
            my_logger.debug("Scraping Completed")

    def scrape_daily_link(self):
        article_links = []
        author_links = []
        categories = []
        page_no = 1
        try:
            my_logger.warn("Start")
            start = time.time()
            while True:
                url = self.DAILY_STARTING_URL.format(page_no)
                source = self.open_url(url)
                daily_article_info = parser.parse_daily_links_and_info(source, self.BASE_URL)
                published_date = daily_article_info["published_date"]
                try:
                    yesterday_index = published_date.index(False)
                except ValueError, err:
                    yesterday_index = -1
                if yesterday_index == -1:
                    article_links.extend(daily_article_info["article_links"])
                    author_links.extend(daily_article_info["author_links"])
                    categories.extend(daily_article_info["categories"])
                    if not self.is_first_scrape and page_no == 2:
                        break
                else:
                    article_links.extend(daily_article_info["article_links"][:yesterday_index])
                    author_links.extend(daily_article_info["author_links"][:yesterday_index])
                    categories.extend(daily_article_info["categories"][:yesterday_index])
                    my_logger.debug("Time taken: {}\n".format(\
                            utility.time_delta(start)))
                    break
                page_no += 1
        except Exception, err:
            my_logger.debug("Problem in scraping daily links")
            my_logger.warn(str(err))
        try:
            my_logger.warn("Save")
            start = time.time()
            for index, article_link in enumerate(article_links):
                author_name, author_url = author_links[index]
                final_link = self.get_final_url(author_url)
                blogger_hash = self.get_blogger_id(final_link)
                if not db.author_exists(blogger_hash):
                    self.save_author_info(final_link, blogger_hash, author_name)
                author = db.duplicate(blogger_hash)
                if db.article_exists(author, article_link):
                    break # Indicates the last url fetched
                else:
                    db.save_article_with_category(author, article_link, categories[index])
            my_logger.debug("Time taken: {}\n".format(\
                            utility.time_delta(start)))
        except Exception, err:
            my_logger.debug("Problem in saving article and author")
            my_logger.warn(str(err))


    def scrape(self):
        try:
            if utility.is_tomorrow():
                self.scheduler.shutdown()
                my_logger.debug("Ending Blogger Scheduler")
            
            # # scrape style category links
            self.scrape_style_category_links()

            # # scrape author links
            self.scrape_author_links()

            # # scrape author info
            self.scrape_author_info()

            # scrape daily links
            self.scrape_daily_link()
        except Exception, err:
            my_logger.warn(str(err))

        # TODO: Apply Thread to scrape multiple urls at once

    def start(self):
        # First scrape of the day full run
        try:
            db.before_request_handler(self.blogger_db)
            self.scrape()
            self.is_first_scrape = False
            self.scheduler.add_job(self.scrape, 'interval', minutes=self.SCHEDULE_INTERVAL)
            self.scheduler.start()
        except Exception, err:
            my_logger.debug("Problem in scheduler")
            my_logger.warn(str(err))
        finally:
            db.after_request_handler()
            try:
                self.scheduler.shutdown()
                my_logger.debug("Ending Blogger Scheduler")
            except Exception, err:
                pass # Already shutdown


def parse_arguments():
    parser = argparse.ArgumentParser(description="Blog Author data")
    parser.add_argument('--root_dir', action='store', dest='root_dir',
                        required=True, help='Root Directory')
    parser.add_argument('--config', action='store', dest='config',
                        required=True, help='Config File name')
    result = parser.parse_args()
    return result

if __name__ == "__main__":
    '''
       Usage pattern: spider.py --config=... --root_dir=xxx
    '''
    # parse command-line arguments
    result = parse_arguments()

    # params 
    root_dir = result.root_dir
    config_file = result.config
    config = utility.read_config(config_file)

    # spider
    spider = BloggerSpider(root_dir, config)
    spider.start()
