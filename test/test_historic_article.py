import logging
import os
import rethinkdb as r
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import historic_article

from datetime import datetime, timedelta
from mock import patch, Mock
from mockthink import MockThink


class TestDataSeparator(unittest.TestCase):

	def setUp(self):
		self.DB_NAME = 'test'
		self.TABLE_ARTICLE_INFO = 'article_info'
		self.TABLE_HISTORIC_ARTICLE_INFO = 'historic_article_info'
		self.N_DAYS = 30 # any random integer 
		self.PAST_DAYS = self.N_DAYS + 5 # to making article of this past days
	@patch('historic_article.logger')
	@patch('historic_article.get_rethink_connection')
	def testArticleSeparator(self, mock_get_rethink, mock_my_logger):
		''' testing that long past articles are moved to historic table '''

		# getting n_days ago date
		date_N_days_ago = datetime.today() - timedelta(days=self.PAST_DAYS)
		date_N_days_ago = date_N_days_ago.date().strftime('%a, %b. %d')

		db = MockThink({'dbs': {
							self.DB_NAME: {
								'tables': {
									self.TABLE_ARTICLE_INFO:[{"id": "de01fdee-9d92-4610-af13-ba9337966f5e" ,
														"update_date": date_N_days_ago},
													{"id": "de01fdee-9d92-4610-af13-ba9337966f5e" ,
														"update_date": "Thu, Dec. 20"}],
									self.TABLE_HISTORIC_ARTICLE_INFO:[]
								}
							}
						}})

		config = 'any url'
		with db.connect() as conn:
			mock_get_rethink.return_value = conn
			historic_article.article_separator(config, self.N_DAYS)
			count = r.db(self.DB_NAME).table(self.TABLE_HISTORIC_ARTICLE_INFO).count().run(conn)
			self.assertEqual(count, 1)		

if __name__ == "__main__":
	unittest.main()

