'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
from nameparser import HumanName
import re
import boto3
import time


state_abbreviation = 'AZ'
database_table_name = 'legislator_template_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://webscraper.io'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    path = '/test-sites/e-commerce/allinone'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    urls = [base_url + prod_path['href']
            for prod_path in soup.findAll('a', {'class': 'title'})]

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls


def scrape(url):
    # Send request to website
    page = scraper_utils.request(url)
    row = scraper_utils.initialize_row()

    # ... Collect data from page

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Scrape data from collected URLs serially, which is slower:
    # data = [scrape(url) for url in urls]
    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
