import argparse
import json
import logging
import logging.handlers
import os
import rethinkdb as r
import time
import uuid
from newspaper import Article
from selenium import webdriver
from utils import read_config
from utils.connections import get_rethink_connection

LOG_FILENAME = '/tmp/seekingalpha.log'

my_logger = logging.getLogger('intellimind')
my_logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                               mode='a',
                                               )
my_logger.addHandler(handler)

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
	
	article_list = []
	for author in authors_info_list:
		try:
			article = {}
			index = authors_info_list.index(author)
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
			article['author_id'] = author['user_id']
			my_logger.info("Processing info for :" + author_name)
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
				author['description'] = browser.find_element_by_xpath("//p[@class='profile-bio-truncate']").text
				author['since'] = (clean_text(since).split(':')[1]).strip()
				company = browser.find_element_by_xpath("//div[@class='about-company']")
				author['firm_name'] = clean_text(company)		
			time.sleep(1)		
		except Exception as e:
			continue	
		finally:
			article_list.append(article)
			authors_info_list.pop(index)
			authors_info_list.insert(index,author)
	return article_list,authors_info_list

def store_author_data(config, authors_info_list):
	'''
		store author data to rethinkdb
	'''

	for author in authors_info_list:
		conn = get_rethink_connection(config)
		authors = r.table('author_info').run(conn)
		authors_names = [ person['name'] for person in authors]
		if not author['name'] in authors_names:
			inserted = r.table('author_info').insert(author).run(conn)
			my_logger.info("Inserted Author:%s" % author['name'])

def store_story_data(config, story_list, articles_info_list):
	'''
		store articles and story data
	'''
	
	for story in story_list:
		if story['text'] == "":
			continue
		conn = get_rethink_connection(config)
		stories = r.table('story_info').run(conn)
		story_names = [ story_info['title'] for story_info in stories]
		if not story['title'] in story_names:
			index = story_list.index(story)
			r.table('story_info').insert(story,conflict = "replace").run(conn)
			r.table('article_info').insert(articles_info_list[index], conflict="replace").run(conn)
			my_logger.info("Inserted Story:%s" % story['title'])
		
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

def scrape_story_info(delay, story_list, root_dir):
	'''
		scrape for story text
	'''
		
	for story in story_list:
		try:
			index = story_list.index(story)
			article = Article(story['url'])
			time.sleep(1)
			article.download()
			article.html
			article.parse()
			story['text'] = article.text
			story.pop('url')
			my_logger.info("Article:%s" % article.title) 
			time.sleep(delay)
			
		except Exception as e:
			continue
		finally:
			story_list.pop(index)
			story_list.insert(index,story)
	return story_list
	
def scrape_article_info(browser, article_list):
	'''
		scrape article information categorywise
	'''
	
	story_list = []
	articles_info_list = []
	count = 0
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
			
			updated_date = article_info.find_element_by_xpath("//div[@class='author-article-info']")
			article['update_date'] = clean_text(updated_date.find_element_by_tag_name('span'))
			article['author_id'] = author_id
			article['story_id'] = str(uuid.uuid4())
			story['story_id'] = article['story_id']
			story['url'] = article_url
			story['title'] = article_info.find_element_by_tag_name("a").text
			story['cat'] = category
			story['sub_cat'] = sub_cat
			count = count + 1
			story_list.append(story)
			articles_info_list.append(article)
	my_logger.info('No of articles :%s' % count)
	return story_list,articles_info_list
	
def store_json_data(authors_list, category, root_dir):
	'''
		store data to jsonfile
	'''

	for author in authors_list:
		path_to_dir = os.path.join(root_dir)
		if not os.path.exists(path_to_dir):
			os.makedirs(path_to_dir)
		author_path = os.path.join(path_to_dir,"%s.json"% author['name'])
		with open(author_path,'w') as fp:
			json.dump(author,fp)
	
def scrape_records(browser, url_page, root_dir, config):
	'''
		scrape records for authorsn

	'''

	try:
		browser.get(url_page)
		my_logger.info('START')
		delay = config.get('SEEKINGALPHA','DELAY')
		tbodies = browser.find_elements_by_tag_name('tbody')
		authors_info_list = []
		author_names_list = []
		count = 0

		my_logger.info('Fetching Authors List.....')
		
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
		for author in authors_info_list:
			if not author['name'] in author_names_list:
				author_names_list.append(author['name'])
				authors_info.append(author)
	
		# scrape & store data
		article_list,authors_info = scrape_author_info(browser,authors_info)
		store_author_data(config, authors_info)
		story_list,articles_info_list = scrape_article_info(browser, article_list)
		story_list = scrape_story_info(delay, story_list, root_dir)
		store_story_data(config, story_list, articles_info_list)
		
	except Exception as e:
		my_logger.error("%s" % e.message)
	finally:
		my_logger.info('FINISH')
		browser.quit()

if __name__ == '__main__':

	'''
		python scrape.py --config=XXX --root_dir=YYY
	'''

	# parse arguments
	result = parse_arguments()
	config = read_config(result.config)
	root_dir = result.root_dir
	firefox = config.get('FIREFOX','DRIVER_PATH')
	browser = webdriver.Firefox(executable_path=firefox)
	url_page = config.get('SEEKINGALPHA','LEADERS_URL')
	scrape_records(browser,url_page,root_dir, config)
