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


import time
import boto3
import re
from nameparser import HumanName
from pprint import pprint
from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from legislator_scraper_utils import USStateLegislatorScraperUtils
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
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row.
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.
    party = 'Republican'
    row.party_id = scraper_utils.get_party_id(party)
    row.party = party

    # Other than that, you can replace this statement with the rest of your scraper logic.

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
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')

