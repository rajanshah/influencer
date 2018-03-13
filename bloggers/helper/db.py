# -*- coding: utf-8 -*-

import logging
import uuid
from datetime import date, timedelta

from model import Article, Author, database, StyleCategory

LOG_FILENAME = '/tmp/intellimind.log'

my_logger = logging.getLogger('intellimind')
my_logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                               maxBytes=20,
                                               backupCount=5,
                                               )
my_logger.addHandler(handler)

def before_request_handler():
    try:
        database.connect()
        database.create_tables([Author, StyleCategory, Article])
        my_logger.info("Tables Created")
    except Exception, e:
        database.connect()
        my_logger.warn(str(e))

def after_request_handler():
    my_logger.info("Database activity closed")
    database.close()

def save_author(author_info):
    if author_exists(author_info['blogger_hash']):
        author = duplicate(author_info['blogger_hash'])

        author.name=author_info['name']
        author.short_name=author_info['short_name']
        author.firm_name=author_info['firm_name']
        author.description=author_info['description']
        author.picture=author_info['picture']
        author.followers=author_info['followers']
        author.ticker=author_info['ticker']
        author.url=author_info['url']
        author.updated_date=author_info['updated_date']
        
        author.save()
        return author
    else:
        try:
            with database.transaction():
                author = Author.create(
                           blogger_hash=author_info['blogger_hash'],
                           blogger_id=uuid.uuid4(),
                           user_id=author_info['user_id'],
                           name=author_info['name'],
                           short_name=author_info['short_name'],
                           firm_name=author_info['firm_name'],
                           description=author_info['description'],
                           since=author_info['since'],
                           picture=author_info['picture'],
                           followers=author_info['followers'],
                           ticker=author_info['ticker'],
                           url=author_info['url'],
                           updated_date=author_info['updated_date']
                       )
                return author
        except Exception, e:
            my_logger.debug("Problem in saving author")
            my_logger.warn(str(e))

def save_style_category(author, style, category):
    try:
        with database.transaction():
            style_category = StyleCategory(
                                author=author,
                                style=style,
                                category=category
                            )
            style_category.save()
    except Exception, e:
        my_logger.debug("Problem in saving style category")
        my_logger.warn(str(e))

def save_article(author, article_url):
    try:
        with database.transaction():
            Article.create(
                    author=author,
                    article_url=article_url,
                    updated_date=date.today(),
                    story_id=uuid.uuid4()
               )
    except Exception, e:
        my_logger.debug("Problem in saving article")
        my_logger.warn(str(e))

def author_exists(blogger_hash):
    sq = Author.select().where(Author.blogger_hash==blogger_hash)
    return sq.exists()

def within_cache(blogger_hash, delta):
    author = Author.select().where(Author.blogger_hash==blogger_hash).get()
    return (date.today() - author.updated_date).days <= delta

def duplicate(blogger_hash):
    try:
        return Author.select().where(Author.blogger_hash==blogger_hash).get()
    except Exception, e:
        my_logger.warn(str(e))

def reentry(author, style, category):
    try:
        sq = StyleCategory.select().where(StyleCategory.author==author.id,\
                    StyleCategory.style==style, \
                    StyleCategory.category==category)
        return sq.exists()
    except Exception, e:
        my_logger.warn(str(e))

def article_exists(author, article_url):
    sq = Article.select().where(Article.author==author, \
                                Article.article_url==article_url)
    return sq.exists()


