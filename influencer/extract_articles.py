import argparse
import os
import rethinkdb as r
import sys
import time
import uuid
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup

from urllib.parse import urlparse

import traceback

from time import strptime
from utils import read_config
from utils.connections import get_rethink_connection


from helpers.log import get_logger

logger = get_logger("influencer")


TABLE_STORY_INFO = "story_info"
TABLE_ARTICLE_INFO = "article_info"
TABLE_AUTHOR_INFO = "author_info"
DB_NAME = "test"

db_cache = {}


def clean_text(text):
	text = "".join([i if ord(i) < 128 else " " for i in text])
	return " ".join(text.split())


def parse_arguments():
	""" parse arguments """

	# parse arguments
	parser = argparse.ArgumentParser(description="Extract authors information")
	parser.add_argument(
		"--config", action="store", dest="config", required=True, help="config"
	)
	result = parser.parse_args()
	return result


def extract_articles(soup, author_url):

	articles = []
	for article in soup.find("div", {"class": "latest-articles-content"}).find_all(
		"div", {"class": "author-single-article"},
	):

		title = article.find("div", {"class": "author-article-title"},)
		if not title:
			continue
		link = title.find("a")

		updated_date = clean_text(
			article.find("div", {"class": "author-article-info"}).find("span").text
		)

		try:
			# making sure it got exact format like e.g. 'Thu, Dec. 20'
			strptime(updated_date, "%a, %b. %d")
		except ValueError as e:
			# in case of update_date does not match format like
			# e.g (Yesterday or Today) then change it to today
			updated_date = datetime.today().strftime("%a, %b. %d")

		articles.append(
			{
				"title": link.text,
				"url": complete_relative_url(url, link["href"]),
				"update_date": updated_date,
				"author_url": author_url,
			}
		)

	return articles


def scrape_author_info(driver, author):
	"""
		author information extraction and return articles from author
	"""

	logger.info("scraping authors info")

	article_list = []

	try:
		driver.get(author["url"])
		element_present = EC.presence_of_element_located(
			(By.CLASS_NAME, "author-article-title")
		)
		WebDriverWait(driver, 10).until(element_present)
		soup = BeautifulSoup(driver.page_source, "html.parser")

		author["followers"] = soup.find("a", {"class": "followers"}).find("i").text
		author["picture"] = soup.find("div", {"class": "about-author-image"}).find(
			"img"
		)["src"]

		article_list = extract_articles(soup, author["url"])

		author["user_id"] = str(uuid.uuid4())
		logger.info("Processing info for :" + author["name"])

		more = soup.find("p", {"class": "profile-bio-truncate"}).find("a")

		if more:
			more_button = driver.find_element_by_partial_link_text("more")
			more_button.click()
			time.sleep(2)

			soup = BeautifulSoup(driver.page_source, "html.parser")
			author["since"] = clean_text(
				soup.find("div", {"class": "contributor-since"}).text
			)

			author["description"] = clean_text(
				soup.find("div", {"class": "author-bio"}).text
			)

			company_text = soup.find("div", {"class": "company"})
			if company_text:
				company_text = clean_text(company_text.text).split(":")
				author["firm_name"] = (
					"" if len(company_text) == 1 else company_text[1].strip()
				)
			else:
				author["firm_name"] = ""
		else:
			author["description"] = clean_text(
				soup.find("p", {"class": "profile-bio-truncate"}).text
			)
			author["since"] = (
				clean_text(soup.find("div", {"class": "about-member-since"}).text)
				.split(":")[-1]
				.strip()
			)
			company_text = clean_text(
				soup.find("div", {"class": "about-company"}).text
			).split(":")

			author["firm_name"] = (
				"" if len(company_text) == 1 else company_text[1].strip()
			)

	except Exception as e:
		logger.error(
			f"Could not scrape author info {e.args}, {author['url']} {traceback.format_exc()}"
		)

	return article_list


def scrape_story_info(driver, article):
	"""
		scrape for story text
	"""
	driver.get(article["url"])
	element_present = EC.presence_of_element_located((By.CLASS_NAME, "sa-art"))
	WebDriverWait(driver, 10).until(element_present)

	soup = BeautifulSoup(driver.page_source, "html.parser")
	article["text"] = clean_text(soup.find("div", {"class": "sa-art"}).text)


def complete_relative_url(base_url, relative_url):
	o = urlparse(base_url)
	return f"{o.scheme}://{o.hostname}/{relative_url.strip('/')}"


def get_authors_list(soup, base_url):
	authors = []
	for author_div in soup.find("div", {"class": "portfolio-main"}).find_all(
		"div", {"class": "author-single-follow-wrapper"}
	):
		author = {
			"name": author_div.find("a", {"class": "followers-name"}).text,
			"url": complete_relative_url(
				base_url, author_div.find("a", {"class": "followers-name"})["href"]
			),
		}
		if "/author/" in author["url"]:
			authors.append(author)
	return authors


def scrape_authors_list(driver, url):
	driver.get(url)
	element_present = EC.presence_of_element_located((By.CLASS_NAME, "portfolio-main"))
	WebDriverWait(driver, 10).until(element_present)

	# scroll to the bottom of the page
	SCROLL_PAUSE_TIME = 5
	last_height = driver.execute_script("return document.body.scrollHeight")
	while True:
		# Scroll down to bottom
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		# Wait to load page
		time.sleep(SCROLL_PAUSE_TIME)

		# Calculate new scroll height and compare with last scroll height
		new_height = driver.execute_script("return document.body.scrollHeight")
		if new_height == last_height:
			break
		last_height = new_height

	soup = BeautifulSoup(driver.page_source, "html.parser")
	authors = get_authors_list(soup, url)

	return authors


def record_exists(conn, key, value, table_name):
	return r.db(DB_NAME).table(table_name).filter({key: value}).count().run(conn) > 0


def insert_item(conn, item, table_name):
	r.db(DB_NAME).table(table_name).insert(item).run(conn)


def update_db_cache():
	""" To update cache from database for optimization """

	try:
		logger.info("Connecting to Rethink DB for update_db_cache")
		conn = get_rethink_connection(config)
		logger.info("Connected")
	except Exception as e:
		logger.info("Error while caching urls:" + str(e))
		return

	db_cache = {"story": {"title": []}, "author": {"name": [], "user_id": []}}

	stories = r.db(DB_NAME).table(TABLE_STORY_INFO).run(conn)
	for story in stories:
		db_cache["story"]["title"].append(story.get("title"))
	logger.info("cache updated stories:" + str(len(db_cache["story"]["title"])))

	authors = r.db(DB_NAME).table(TABLE_AUTHOR_INFO).run(conn)
	for author in authors:
		db_cache["author"]["name"].append(author["name"])
		db_cache["author"]["user_id"].append(author["user_id"])
	logger.info("cache updated authors :" + str(len(db_cache["author"]["name"])))


def get_firefox_driver(driver_path):
	options = webdriver.FirefoxOptions()
	options.headless = True
	return webdriver.Firefox(executable_path=driver_path, firefox_options=options)


if __name__ == "__main__":
	"""
		python scrape.py --config=XXX
	"""

	# parse arguments
	result = parse_arguments()
	config = read_config(result.config)

	driver_path = config.get("FIREFOX", "DRIVER_PATH")
	url = config.get("SEEKINGALPHA", "LEADERS_URL")
	# LEADERS_URL is set to https://seekingalpha.com/author/sa-eli-hoffmann/following

	try:
		logger.info("Connecting to Rethink DB for update_db_cache")
		conn = get_rethink_connection(config)
		logger.info("Connected")
	except Exception as e:
		logger.info("Error while caching urls:" + str(e))
		exit()

	try:
		driver = get_firefox_driver(driver_path)
		authors_list = scrape_authors_list(driver, url)

		for author in authors_list:
			articles = scrape_author_info(driver, author)
			if not record_exists(
				conn, "url", author["url"], table_name=TABLE_AUTHOR_INFO
			):
				insert_item(conn, author, table_name=TABLE_AUTHOR_INFO)

			for article in articles:
				if not record_exists(
					conn, "url", article["url"], table_name=TABLE_ARTICLE_INFO
				):
					scrape_story_info(driver, article)
					insert_item(conn, article, table_name=TABLE_ARTICLE_INFO)
	except Exception as e:
		logger.error(f"Scraping failed {e.args}, {traceback.format_exc()}")
	finally:
		driver.close()
