# -*- coding: utf-8 -*-

import logging
import os
import tempfile
import unittest

from spider import BloggerSpider
from utils.mechanizebrowser import MechanizeBrowser
from utils import utility

class BloggerTest(unittest.TestCase):
    def setUp(self):
        self.BASE_URL = "http://seekingalpha.com"
        self.STARTING_URL = "http://seekingalpha.com/leader-board/opinion-leaders"
        self.CATEGORY_URL = "http://seekingalpha.com/opinion-leaders/long-ideas"
        # Temp config file
        self.temp = tempfile.NamedTemporaryFile(mode='w+t')
        try:
            self.temp.writelines([
                "[BLOGGER]\n"
                "BLOGGER_DIR=/var/tmp/data/blogger\n"
                "BASE_URL=http://seekingalpha.com\n"
                "STARTING_URL=http://seekingalpha.com/leader-board/opinion-leaders\n"
                "STORAGE_FOLDER=storage\n"
                "STYLE_CATEGORY_FILENAME=styleCategoryLinks.json\n"
                "DEFAULT_DELAY=5\n"
                ])
        except:
            logging.warn("Problem in creating temp file")

        self.temp.seek(0)
        self.spider = BloggerSpider(self.temp.name)

    def tearDown(self):
        self.temp.close()

    def testSiteUp(self):
        response = self.spider.get_response(self.STARTING_URL)
        self.failUnlessEqual(response.code, 200)

    def testTableCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        tables = tree.cssselect('table.ld_table')
        self.failUnlessEqual(len(tables), 5)

    def testInvestingIdeas(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_investing_ideas')[0]
        table_heading = table.cssselect('tr > th')[0]
        self.failUnlessEqual(table_heading.text_content() ,"Investing Ideas")

    def testInvestingIdeas(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_investing_ideas')[0]
        table_heading = table.cssselect('tr > th')[0]
        self.failUnlessEqual(table_heading.text_content() ,"Investing Ideas")

    def testColumnsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_macro_view')[0]
        columns = table.cssselect('tr > th')
        self.failUnlessEqual(len(columns) ,6)

    def testInvestingIdeasRowsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_investing_ideas')[0]
        rows = table.cssselect('tr')
        self.failUnlessEqual(len(rows) ,7)

    def testDividendsRowsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_investing_income')[0]
        rows = table.cssselect('tr')
        self.failUnlessEqual(len(rows) ,8)

    def testMacroRowsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_macro_view')[0]
        rows = table.cssselect('tr')
        self.failUnlessEqual(len(rows) ,8)

    def testSectorsRowsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_sectors')[0]
        rows = table.cssselect('tr')
        self.failUnlessEqual(len(rows) ,10)

    def testETFRowsCount(self):
        source = self.spider.open_url(self.STARTING_URL)
        tree = utility.get_tree(source)
        table = tree.cssselect(\
                    'div #content_wrapper table.table_etfs')[0]
        rows = table.cssselect('tr')
        self.failUnlessEqual(len(rows) ,7)

    def testCategoryCrawler(self):
        links = self.spider.author_links(self.CATEGORY_URL)
        self.failIfEqual(links, [])

    def testCategoryCount(self):
        links = self.spider.author_links(self.CATEGORY_URL)
        self.failUnlessEqual(len(links), 100)



if __name__ == "__main__":
    unittest.main()
