'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from legislator_scraper_utils import CadFedLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3


scraper_utils = CadFedLegislatorScraperUtils()

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Url we are scraping: https://www.azleg.gov/memberroster/
    base_url = 'https://www.azleg.gov'
    path = '/memberroster/'
    scrape_url = base_url + path
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table', {'id': 'HouseRoster'})

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for tr in table.findAll('tr')[1:11]:
        a = tr.find('a', {'class':'roster-tooltip'})
        urls.append(a['href'])
    
    return urls


def scrape(url):
    
    row = scraper_utils.initialize_row()

    row.source_url = url

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    bio_container = soup.find('div', {'class': 'one-half first'})

    party = 'Liberal'
    
    row.party_id = scraper_utils.get_party_id(party) 
    row.party = party

    name_full = bio_container.find('h3').text

    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
