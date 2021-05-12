'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import boto3
import re
from nameparser import HumanName
from pprint import pprint
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import time
from scraper_utils import CAProvTerrLegislatorScraperUtils


prov_abbreviation = 'NS'
database_table_name = 'ca_ns_legislators'

scraper_utils = CAProvTerrLegislatorScraperUtils(
    prov_abbreviation, database_table_name)

base_url = 'https://nslegislature.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Url we are scraping: https://nslegislature.ca/members/profiles
    path = '/members/profiles'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    members_view = soup.find('div', {'class': 'view-content'})

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for tr in members_view.findAll('a')[0:11]:
        a = tr
        urls.append(base_url + a['href'])

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls


def get_party(bio_container, row):
    party = bio_container.find('span', {'class': 'party-name'}).text

    if party == 'PC':
        party = 'Progressive Conservative'
    if party == 'NDP':
        party = 'New Democratic'

    row.party_id = scraper_utils.get_party_id(party)
    row.party = party


def get_name(bio_container, row):
    name_full = bio_container.find('div', {'class': 'views-field-field-last-name'}).text

    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix


def get_riding(bio_container, row):
    riding = bio_container.find('td', {'class': 'views-field-field-constituency'}).text
    row.riding = riding


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

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url
    # like so:
    row.source_url = url

    # get region
    region = scraper_utils.get_region(prov_abbreviation)
    row.region = region

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    bio_container = soup.find('div', {'class': 'panels-flexible-region-mla-profile-current-center'})

    get_party(bio_container, row)
    get_name(bio_container, row)
    get_riding(bio_container, row)

    # Get phone number
    bio_text = bio_container.text
    phone_number = re.findall(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', bio_text)[0]
    phone_number = [{'office': '', 'number': phone_number}]

    row.phone_numbers = phone_number

    # There's other stuff we can gather on the page, but this will do for demo purposes

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) since the crawl delay is very large (ie: {crawl_delay} seconds). If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    print('Scraping data...')
    with Pool() as pool:
        data = pool.map(scrape, urls)
    print('Scraping complete')

    # Once we collect the data, we'll write it to the database.
    #scraper_utils.write_data(data)

    print(f'Scraper ran succesfully!')
