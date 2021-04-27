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
from scraper_utils import USStateLegislatorScraperUtils
import time


state_abbreviation = 'AZ'
database_table_name = 'demo_template_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://www.azleg.gov'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Url we are scraping: https://www.azleg.gov/memberroster/
    path = '/memberroster/'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.find('table', {'id': 'HouseRoster'})

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for tr in table.findAll('tr')[1:11]:
        a = tr.find('a', {'class': 'roster-tooltip'})
        urls.append(a['href'])

    # Delay so we do not overburden servers
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

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url
    # like so:
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    bio_container = soup.find('div', {'class': 'one-half first'})

    party = bio_container.find('a').text
    # Remove the 'ic' from the Democratic party
    if party == 'Democratic':
        party = party[:-2]

    row.party_id = scraper_utils.get_party_id(party)
    row.party = party

    # Other than that, you can replace this statement with the rest of your scraper logic.
    # Get names
    name_full = bio_container.find('h3').text

    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix

    # Get district
    district = bio_container.find('a', {'class': 'district-tooltip'}).text
    district = district.replace('District ', '')
    row.district = district

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
    scraper_utils.write_data(data)

    print(f'Scraper ran succesfully!')
