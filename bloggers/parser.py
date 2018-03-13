# -*- coding: utf-8 -*-

import json
import logging
import logging.handlers
import traceback
import urlparse

from xsutils.utility import get_tree

LOG_FILENAME = '/tmp/intellimind.log'

my_logger = logging.getLogger('intellimind')
my_logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                               'a'
                                               )
my_logger.addHandler(handler)

def normalize(seed_url, link):
    return urlparse.urljoin(seed_url, link)

def parse_links(source, base_url):
    """Scrapes the page to extract all the links of each style.
    {
        "style":[{"category":DetailPageURL}],
        "style":[{"category":DetailPageURL}],
        .
        .
        .
    }
    """
    links = {}
    try:
        tree = get_tree(source)
        table_css = "div#content_wrapper table"
        tables = tree.cssselect(table_css)
        for table in tables:
            tr = table.cssselect("tr")
            style = tr[0].cssselect("th")[0].text_content().strip()
            links[style] = []
            for tr in tr[1:]:
                category_links = []
                category_td = tr.cssselect("td")[0]
                category = category_td.cssselect("div")[0].text_content().strip()
                href = category_td.cssselect("a")[0].get('href')
                links[style].append([category, normalize(base_url, href)])
    except Exception, e:
        my_logger.debug("Problem parsing for style and categories link")
        my_logger.warn(str(e))
    finally:
        return links

def parse_next_category_link(source, base_url):
    """Scrapes the page to extract the link to next leaderboards
    """
    try:
        tree = get_tree(source)
        next_link = tree.cssselect(\
                        "div#content_wrapper div.more_leaderboards > a")[-1]
        return normalize(base_url, next_link.get('href'))
    except Exception, e:
        my_logger.debug("Problem in parsing the next category link")
        my_logger.warn(str(e))
        return ""

def parse_author_links(source, base_url):
    """Scrapes the page to extract all the author links from the page.
    [
        ("author_name",AuthorDetailURL),
        ("author_name",AuthorDetailURL),
        ...
        ...
    ]
    """
    links = []
    try:
        tree = get_tree(source)
        main_div = tree.cssselect('div#content_wrapper div.ld_page_main')[0]
        top_div_lis = main_div.cssselect('div.ld_top li')
        more_div_lis = main_div.cssselect('div.ld_more li')
        top_div_lis.extend(more_div_lis)
        for li in top_div_lis:
            href = li.cssselect('span > a')[0].get('href')
            author = li.cssselect('span > a')[0].text_content().strip()
            links.append((author, normalize(base_url, href)))
        return links
    except Exception, e:
        my_logger.debug("Problem in parsing author links")
        my_logger.warn(str(e))
    finally:
        return links

def parse_author_info(source):
    """Scrapes the author info from the author page
    {
        "name":"",
        "short_name":"",
        "firm_name":"",
        "description":"",
        "since":year,
        "picture":base64,
        "followers":count
    }
    """
    author_info = {}
    try:
        baseinfo = source.find("baseInfo")
        if baseinfo < 0:
            baseinfo = source.find("SA.Pages.Profile.init")
            if baseinfo < 0:
                return author_info
        start = baseinfo + source[baseinfo:].index("{")
        end = start + source[start:].index(");")
        info_string = source[start:end]

        info = json.loads(info_string)
        
        author_info['name'] = info['profile_info']['name']
        author_info['short_name'] = info['profile_info']['nick']
        author_info['user_id'] = info['profile_info'].get('user_id') or \
                                info.get('profile_id')
        author_info['since'] = info['profile_info']['member_since']
        author_info['picture'] = info['profile_info']['picture_url']
        author_info['followers'] = int(info['object_count']['followers_count'])
        tree = get_tree(source)
        desc_css = "div#profile-header div.about-author-desc > div"
        description = tree.cssselect(desc_css)
        if len(description) > 1:
            spans = description[1].cssselect('p > span')
            if len(spans) > 1:
                author_info['description'] = spans[2].text_content().strip()
            else:
                author_info['description'] = \
                    description[0].text_content().strip()
        else:
            author_info['description'] = description[0].get('data-bio').strip()
        firm_css = "div#profile-header div.about-company > span:nth-child(2)"
        firm_name = tree.cssselect(firm_css)
        author_info['firm_name'] = firm_name[0].text_content().strip() if \
                                firm_name else ""
    except ValueError as e:
        my_logger.debug("Cannot convert string to JSON")
    except Exception, e:
        my_logger.debug("Problem in parsing author info")
        my_logger.warn(str(e))
    finally:
        return author_info

def parse_coverage_info(source):
    """Scrapes the tickers covered by the author
    {
        tickers: [....]
    }
    """
    try:
        tickers = json.loads(source)
        return {"ticker":json.dumps(tickers['counts'].keys())}
    except Exception, e:
        my_logger.debug("Problem in parsing tickers")
        my_logger.warn(str(e))
        return {"ticker":json.dumps([])}

def parse_article_page(source):
    json_content = json.loads(source)
    return json_content

def parse_html_article_links(source, base_url):
    """Scrapes the article links from the old article page
    """
    try:
        tree = get_tree(source)
        links = []
        for a in tree.cssselect('div.author-article-title > a'):
            href = a.get('href')
            if a.get('data-uri'):
                href = a.get('data-uri')
            links.append(normalize(base_url, href))
        return {"article_url":links}
    except Exception, e:
        my_logger.debug("Problem in parsing article links in old page")
        my_logger.warn(str(e))
        return {"article_url":[]}

def parse_json_article_links(source, base_url):
    """Scrapes the article links from the old article page
    """
    try:
        json_content = json.loads(source)
        links = []
        time = json_content['time']
        for card in json_content['cards']:
            tree = get_tree(card['content'])
            try:
                href = tree.cssselect("div > a")[0].get('href')
                links.append(normalize(base_url, href))
            except Exception, e:
                my_logger.debug("Problem in article url {} with id: {}".format(
                      card['user']['id'], card['id']))
        return {"article_url":links, "time":int(time)}
    except Exception, e:
        my_logger.debug("Problem in parsing article links in new page")
        my_logger.warn(str(e))
        return {"article_url":[], "time":0}

def parse_links_from_all_articles(source, base_url):
    """Scrapes daily article links from all articles
    """
    try:
        tree = get_tree(source)
        article_url_css = "ul#articles-list > li > div:nth-child(2) > a"
        article_links = [normalize(base_url, a.get("href")) for a in tree.cssselect(article_url_css)]
        return article_links
    except Exception, err:
        my_logger.debug("Problem in scraping daily article link")
        my_logger.warn(str(err))
        return []

def _clean_author_url(link):
    return link.rpartition("/")[0]

def parse_authors_from_all_articles(source, base_url):
    try:
        tree = get_tree(source)
        author_url_css = "ul#articles-list > li > div:nth-child(2) > div > a"
        author_links = [(a.text_content().strip() ,normalize(base_url, _clean_author_url(a.get("href")))) for a in tree.cssselect(author_url_css)]
        return author_links
    except Exception, err:
        my_logger.debug("Problem in scraping daily author links")
        my_logger.warn(str(err))
        return []

def _is_published_today(_date):
    day = _date.split(",")[0].strip()
    return day == "Today"

def parse_article_published_date(source):
    try:
        published_dates = []
        tree = get_tree(source)
        article_info_css = "ul#articles-list > li > div:nth-child(2) > div"
        article_info_divs = tree.cssselect(article_info_css)
        published_date_xpath = "./span[not(@class)]"
        for div in article_info_divs:
            spans = div.xpath(published_date_xpath)
            if len(spans) > 1:
                published_dates.append(_is_published_today(spans[-1].text_content().strip()))
            else:
                published_dates.append(_is_published_today(spans[0].text_content().strip()))
        return published_dates
    except Exception, err:
        my_logger.debug("Problem in scraping daily article published dates")
        my_logger.warn(str(err))
        return []

def _get_earning_conference_category(article_links):
    earning_conference = []
    for link in article_links:
        if link.endswith("earnings-call-transcript"):
            earning_conference.append("EarningCallTranscript")
        elif link.find("conference") > -1:
            earning_conference.append("Conference")
        else:
            earning_conference.append("")
    return earning_conference

def parse_article_category(source, article_links):
    try:
        categories = []
        tree = get_tree(source)
        article_info_css = "ul#articles-list > li > div:nth-child(2) > div"
        article_info_divs = tree.cssselect(article_info_css)
        editor_pick_css = "span.editors-pick-yellow-text"
        editors_pick = [div.cssselect(editor_pick_css) for div in article_info_divs]
        earning_conference = _get_earning_conference_category(article_links)
        for index, item in enumerate(earning_conference):
            if item:
                categories.append(item)
            elif len(editors_pick[index]):
                categories.append("EditorsPick")
            else:
                categories.append("Normal")
        return categories
    except Exception, err:
        my_logger.debug("Problem in scraping article categories")
        my_logger.warn(str(err))
        return []

def parse_daily_links_and_info(source, base_url):
    """Scrapes daily articles links and info
    """
    article_links = parse_links_from_all_articles(source, base_url)
    daily_info = {
        "article_links": article_links,
        "author_links": parse_authors_from_all_articles(source, base_url),
        "published_date": parse_article_published_date(source),
        "categories": parse_article_category(source, article_links)
    }
    return daily_info
