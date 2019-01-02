import os
import sys
import logging
import unittest
import rethinkdb as r

from mock import patch, Mock
from mockthink import MockThink

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_extract import get_rethink_connection

from pipeline_extract import clean_text
from pipeline_extract import fetch_authors

from pipeline_extract import scrape_article_info
from pipeline_extract import scrape_author_info
from pipeline_extract import scrape_records
from pipeline_extract import scrape_story_info

from pipeline_extract import store_author_data
from pipeline_extract import store_story_data

from pipeline_extract import update_db_cache

class TestPipelineExtractor(unittest.TestCase):
	
	def setUp(self):
		self.DB_NAME = 'test'
		self.TABLE_AUTHOR_INFO = 'author_info'
		self.TABLE_STORY_INFO = 'story_info'
		self.TABLE_ARTICLE_INFO = 'article_info'

	@patch('pipeline_extract.logger')
	@patch('pipeline_extract.get_rethink_connection')
	def test_store_author_data(self, mock_get_rethink, mock_my_logger):
		''' testing that it stores authors info currectly '''

		authors_list = [{'name':'abc'},{'name':'xyz'}]
		config = 'any path'
		db = MockThink({'dbs': {
					self.DB_NAME: {
						'tables': {
							self.TABLE_AUTHOR_INFO:[]}}}})

		with db.connect() as conn:
			mock_get_rethink.return_value = conn
			store_author_data(config, authors_list)
			count = r.db(self.DB_NAME).table(self.TABLE_AUTHOR_INFO).count().run(conn)
			self.assertEqual(count, len(authors_list))

	@patch('pipeline_extract.logger')
	@patch('pipeline_extract.get_rethink_connection')
	def test_store_story_data(self, mock_get_rethink, mock_my_logger):
		''' testing that it stores story data currectly '''

		story_list = [{'text':'abc', 'title':'1'},{'text':'xyz', 'title':'2'}]
		article_list = [{'name':'abc'},{'name':'xyz'}]
		config = 'any path'
		db = MockThink({'dbs': {
					self.DB_NAME: {
						'tables': {
							self.TABLE_STORY_INFO:[],
							self.TABLE_ARTICLE_INFO:[]}}}})

		with db.connect() as conn:
			mock_get_rethink.return_value = conn
			store_story_data(config, story_list, article_list)

			count_story = r.db(self.DB_NAME).table(self.TABLE_STORY_INFO).count().run(conn)
			count_article = r.db(self.DB_NAME).table(self.TABLE_ARTICLE_INFO).count().run(conn)

			self.assertEqual(count_story, len(story_list))
			self.assertEqual(count_article, len(article_list))

if __name__ == "__main__":
	unittest.main()

