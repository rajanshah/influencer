import argparse
import json
import logging
import logging.handlers
import os
import rethinkdb as r
import sys
import socket
import schedule
import time
import uuid

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta 
from logging.handlers import SysLogHandler
from newspaper import Article
from selenium import webdriver
from time import strptime
from utils import read_config
from utils.connections import get_rethink_connection

TABLE_STORY_INFO = 'story_info'
TABLE_ARTICLE_INFO = 'article_info'
TABLE_AUTHOR_INFO = 'author_info'
DB_NAME = 'test'

# setting logger
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

db_cache = {}

def parse_arguments():
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description='Process users tweets')
	parser.add_argument('--root_dir', action='store', dest='root_dir',\
		required=True, help='root directory')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config')
	result = parser.parse_args()
	return result

def clean_text(element):
	'''
		clean text for the element
	'''

	text = element.get_attribute('textContent')
	text = text.replace("\n","").strip()
	text = ''.join([i if ord(i) < 128 else ' ' for i in text])
	return text

def scrape_author_info(browser, authors_info_list):
	'''
		author information extraction
	'''
	
	logger.info('scraping authors info')

	article_list = []
	new_authors_info_list = []
	for author in authors_info_list:
		try:
			article = {}
			browser.get(author['url'])
			time.sleep(3)
			author_name = browser.find_element_by_xpath("//div[@class='about-author-name']")
			author_name = clean_text(author_name)
			article['url'] = author.pop("url")
			article['category'] = author.pop("category")
			article['sub_cat'] = author.pop("sub_category")
			followers =  browser.find_element_by_xpath("//i[@class='profile-top-nav-count']")
			author['followers'] = clean_text(followers)
			img = browser.find_element_by_xpath("//img").get_attribute('src')
			author['picture'] = img
			author['user_id'] = str(uuid.uuid4())
			logger.info("Processing info for :" + author_name)
		
			try:
				more = browser.find_element_by_partial_link_text('more')
			except:
				more = None
	
			if more:
				more.click()
				time.sleep(2)
				since = browser.find_element_by_xpath("//div[@class='contributor-since']")
				author['since'] = clean_text(since).split(" ")[-1]
				author_bio = browser.find_element_by_xpath("//div[@class='author-bio']")
				author['description'] = author_bio.text
				company = browser.find_element_by_xpath("//div[@class='company']")
				author['firm_name'] = (clean_text(company).split(":")[1]).strip()
			else:
				since = browser.find_element_by_xpath("//div[@class='about-member-since']")	
				author['description'] = browser.\
					find_element_by_xpath("//p[@class='profile-bio-truncate']").text
				author['since'] = (clean_text(since).split(':')[1]).strip()
				company = browser.find_element_by_xpath("//div[@class='about-company']")
				author['firm_name'] = clean_text(company)		
			time.sleep(1)		
		except Exception as e:
			continue	
		finally:
		
			# avoiding resaving of author data
			if author['name'] not in db_cache['author']['name']:
				new_authors_info_list.append(author)
			else:
				# getting index to find 'user_id' of same author in db_cache
				index = db_cache['author']['name'].index(author['name'])	
				author['user_id'] = db_cache['author']['user_id'][index]
				
			article['author_id'] = author['user_id']
			article_list.append(article)	
			
	logger.info('Number of new authors :' + str(len(new_authors_info_list)))
	return article_list, new_authors_info_list

def store_author_data(config, authors_info_list):
	'''
		store author data to rethinkdb
	'''

	logger.info('saving author info')
	try:
		logger.info('Connecting to Rethink DB')
		conn = get_rethink_connection(config)
		logger.info('Connected')
	except Exception as e:
		logger.info(e)
		return
	
	try:	
		for author in authors_info_list:
			r.db(DB_NAME).table(TABLE_AUTHOR_INFO).insert(author).run(conn)
			logger.info("inserted author :%s" % author['name'])
	except Exception as e:
		logger.info('Error in storing data :'+ str(e))
		return
	
def store_story_data(config, story_list, articles_info_list):
	'''
		store articles and story data
	'''
	
	logger.info('saving story and article data')
	
	try:
		logger.info('Connecting to Rethink TABLE for store_story_data')
		conn = get_rethink_connection(config)
		logger.info('Connected')
	except Exception as e:
		logger.info('Error :'+str(e))
		return

	try:
		for story in story_list:

			# avoid storing empty story or with exception of field 'text'
			if story.get('text') in ["", None]:
				continue
			
			index = story_list.index(story)
			r.db(DB_NAME).table(TABLE_STORY_INFO).\
				insert(story,conflict = "replace").run(conn)
			r.db(DB_NAME).table(TABLE_ARTICLE_INFO).\
				insert(articles_info_list[index], conflict="replace").run(conn)
			logger.info("storing new Story:%s" % story['title'])
	
	except Exception as e:
		logger.info('Error in storing story data' +str(e))

def fetch_authors(tds, category):
	'''
		fetch authors for the category
	'''

	article = tds[0].find_element_by_tag_name('div')
	tds.pop(0)
	
	authors_list = [] 
	for td in tds:
		author_info = {}
		author = td.find_element_by_tag_name('a')
		author_url = author.get_attribute('href')
		author = clean_text(author)
		author_info['name'] = author
		author_info['url'] = author_url
		author_info['category'] = category
		author_info['sub_category'] = article.get_attribute('textContent') 
		authors_list.append(author_info)
	
	return authors_list

def scrape_story_info(delay, story_list, article_info_list, root_dir):
	'''
		scrape for story text
	'''
	
	logger.info('scrapping story info')
	for story in story_list:
		try:
			index = story_list.index(story)
			article = Article(story['url'].strip())
			time.sleep(1)
			article.download()
			article.html
			article.parse()
			story['text'] = article.text
			story.pop('url')
			logger.info("Article:%s" % article.title) 
			time.sleep(delay)
			
		except Exception as e:
			# when failed to download story
			story_list.pop(index)
			article_info_list.pop(index)

	return story_list, article_info_list
	
def scrape_article_info(browser, article_list):
	'''
		scrape article information categorywise
	'''
	

	story_list = []
	articles_info_list = []
	count = 0
	logger.info('Scrapping article info')
	
	try:		
		for articles_info in article_list:
			browser.get(articles_info['url'])
			author_id = articles_info['author_id']
			category = articles_info['category']
			sub_cat = articles_info['sub_cat']
			articles = browser.find_elements_by_xpath("//div[@class='author-single-article']")
			
			for article_info in articles:
				article = {}
				story = {}			
				article_url = article_info.find_element_by_tag_name("a").get_attribute('href')
				article['article_url'] = article_url			
				updated_date = article_info.\
					find_element_by_xpath("//div[@class='author-article-info']")
				article['update_date'] = clean_text(updated_date.\
					find_element_by_tag_name('span'))
		
				try:
					# making sure it got exact format like e.g. 'Thu, Dec. 20'
					strptime(article['update_date'], '%a, %b. %d')
				
				except ValueError as e:

					# in case of update_date does not match format like
					# e.g (Yesterday or Today) then change it to today
					article['update_date'] = datetime.today().strftime('%a, %b. %d')
				
				article['author_id'] = author_id
				article['story_id'] = str(uuid.uuid4())
				story['story_id'] = article['story_id']
				story['url'] = article_url
				story['title'] = article_info.find_element_by_tag_name("a").text
				story['cat'] = category
				story['sub_cat'] = sub_cat
			
				# To avoind refetching of same data
				if story['title'] not in db_cache['story']['title']:
					articles_info_list.append(article)
					story_list.append(story)
					count = count + 1
					
		logger.info('No of new articles fetched : %s' % count)
	except Exception as e :
		logger.info("Excpetion :"+str(e))
	finally:
		return story_list,articles_info_list
	
def scrape_records(browser, url_page, root_dir, config):
	'''
		scrape records for authorsn
	'''

	try:
		browser.get(url_page)
		logger.info('START')
		delay = config.get('SEEKINGALPHA','DELAY')
		tbodies = browser.find_elements_by_tag_name('tbody')
		authors_info_list = []
		author_names_list = []
		
		logger.info('Fetching Authors List ...')
		
		# fetch list of authors
		for tbody in tbodies:
			trs = tbody.find_elements_by_tag_name('tr')
			category = None
 
			# scrape & store data for each type of articles
			for tr in trs:
				tds = None
				ths = None
				tds = tr.find_elements_by_tag_name('td')
				ths = tr.find_elements_by_tag_name('th')
				if ths:
					category = ths[0].text
				if tds:
					authors_list = fetch_authors(tds, category)
				else:
					continue
				
				authors_info_list.extend(authors_list)
		
		# remove duplicates 
		authors_info = []
		for author in authors_info_list[0:3]:
			if not author['name'] in author_names_list:
				author_names_list.append(author['name'])
				authors_info.append(author)
	
		logger.info('Authors list fetched')

		# scrape authors complete info and all articles link
		article_list,authors_info = scrape_author_info(browser,authors_info)
		
		# store data of authors into db
		store_author_data(config, authors_info)
		
		# scarping new articles info from article_list pointing to stack of articles of one by one author 
		story_list,articles_info_list = scrape_article_info(browser, article_list)
		
		# scrapping actual story
		story_list, articles_info_list = scrape_story_info(delay, story_list, articles_info_list , root_dir)
		
		# storing story and article into db
		store_story_data(config, story_list, articles_info_list)
		
	except Exception as e:
		logger.error("Exception :%s" % e)
	finally:
		logger.info('FINISH')
		browser.quit()

def update_db_cache():
	''' To update cache from database for optimization '''

	try:
		logger.info('Connecting to Rethink DB for update_db_cache')
		conn = get_rethink_connection(config)
		logger.info('Connected')
	except Exception as e:
		logger.info('Error while caching urls:'+str(e))
		return
	
	db_cache = {'story':{'title':[]
						}, 
				'author':{'name':[], 
						'user_id':[]
						}
				}

	stories = r.db(DB_NAME).table(TABLE_STORY_INFO).run(conn)
	for story in stories:
		db_cache['story']['title'].append(story.get('title'))
	logger.info('cache updated stories:' + str(len(db_cache['story']['title'])))
	
	authors = r.db(DB_NAME).table(TABLE_AUTHOR_INFO).run(conn)
	for author in authors:
		db_cache['author']['name'].append(author['name'])
		db_cache['author']['user_id'].append(author['user_id'])
	logger.info('cache updated authors :' + str(len(db_cache['author']['name'])))
	

if __name__ == '__main__':
	'''
		python scrape.py --config=XXX --root_dir=YYY	
	'''

	# parse arguments
	result = parse_arguments()
	config = read_config(result.config)
	root_dir = result.root_dir

	# logging configuration
	try:
		logger.info('Connecting to PAPERTRAIL ...')
		syslog = SysLogHandler(address=(config.get('PAPERTRAIL', 'LOG_SERVER'), int(config.get('PAPERTRAIL', 'LOG_PORT'))))
		logger.info('Connected to PAPERTRAIL')
	except:
		logger.info('Error in connecting PaperTrail server')

	hostname = socket.gethostname() if socket.gethostname() else socket.getfqdn()
	formatter = logging.Formatter('{0}:%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s'.format(hostname),'%Y-%m-%d %H:%M:%S')
	syslog.setFormatter(formatter)
	logger.addHandler(syslog)

	firefox = config.get('FIREFOX','DRIVER_PATH')
	browser = webdriver.Firefox(executable_path=firefox)
	url_page = config.get('SEEKINGALPHA','LEADERS_URL')
	wait_time = int(config.get('SEEKINGALPHA','WAIT_TIME'))
	start_time_cache = config.get('SEEKINGALPHA','START_TIME_CACHE')
	
	# setting scheduling
	schedule.every().day.at(start_time_cache).do(update_db_cache)
	update_db_cache()
	schedule.every().hour.do(scrape_records, browser, url_page, root_dir, config)

	
	logger.info('Starting Scheduler ...')
	while True:
		schedule.run_pending()
		time.sleep(wait_time)
	
