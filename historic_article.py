import argparse
import logging
import logging.handlers
import os
import rethinkdb as r
import sys
import socket
import schedule
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
from logging.handlers import SysLogHandler
from time import strptime
from utils import read_config
from utils.connections import get_rethink_connection

DB_NAME = 'test'
TABLE_ARTICLE_INFO = 'article_info'
TABLE_HISTORIC_ARTICLE_INFO = 'historic_article_info'

# setting logger
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_arguments():
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description='Process users tweets')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config')
	result = parser.parse_args()
	
	return result

def article_separator(config, n_days):
	'''
		storing past one month article into different table
	'''

	logger.info('START')

	try:
		logger.info('Connecting to Rethink DB')
		conn = get_rethink_connection(config)
		logger.info('Connected')
	except Exception as e:
		logger.info('Error :'+str(e))
		return

	try:
		articles = r.db(DB_NAME).table(TABLE_ARTICLE_INFO).run(conn)
		
		count = 0
		for article in articles:
			try:
				date = strptime(article['update_date'], '%a, %b. %d')
			except ValueError as e:
				logger.info('Date formate not matched' + str(e))
				continue

			# making all in same format
			article_date = '{}-{}'.format(int(date.tm_mday), int(date.tm_mon))
			date_N_days_ago = datetime.today() - timedelta(days=n_days)
			date_N_days_ago = date_N_days_ago.strftime('%d-%m')

			article_date = datetime.strptime(article_date, '%d-%m')
			date_N_days_ago = datetime.strptime(date_N_days_ago, '%d-%m')
			
			# if given article is N days earlier than move to new table
			if article_date < date_N_days_ago:
				r.db(DB_NAME).table(TABLE_ARTICLE_INFO).\
					get(article['id']).delete().run(conn)
				r.db(DB_NAME).table(TABLE_HISTORIC_ARTICLE_INFO).\
					insert(article).run(conn)
				count += 1

		logger.info('Total Transfer of article : ' + str(count))
		logger.info('FINISH')

	except Exception as e:
		logger.info('Error :' + str(e))
		
if __name__ == "__main__":

	# parse arguments
	result = parse_arguments()
	config = read_config(result.config)
	n_days = int(config.get('HISTORIC_DATA','N_DAYS'))
	start_time = config.get('HISTORIC_DATA','START_TIME_HISTORIC')
	wait_time = int(config.get('HISTORIC_DATA','WAIT_TIME'))
	# Logging configuration
	try:
		logger.info('Connecting to PAPERTRAIL ...')
		syslog = SysLogHandler(address=(config.get('PAPERTRAIL', 'LOG_SERVER'),\
			 int(config.get('PAPERTRAIL', 'LOG_PORT'))))
		logger.info('Connected to PAPERTRAIL')
	except:
		logger.info('Error in connecting PaperTrail server')

	hostname = socket.gethostname() if socket.gethostname() else socket.getfqdn()
	formatter = logging.Formatter('{0}:%(asctime)s.%(msecs)d %(levelname)s %(module)s \
					- %(funcName)s: %(message)s'.format(hostname),'%Y-%m-%d %H:%M:%S')
	syslog.setFormatter(formatter)
	logger.addHandler(syslog)

	# transfering old article data to different table 
	schedule.every().day.at(start_time).do(article_separator,config)

	logger.info('Starting Scheduler ...')
	while True:
		schedule.run_pending()
		time.sleep(wait_time) # wait for checking peding jobs
	