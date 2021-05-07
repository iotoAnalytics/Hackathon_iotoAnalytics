import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
# import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
# from nameparser import HumanName
import re
# import boto3
# import time
# import pandas as pd

state_abbreviation = 'OK'
database_table_name = 'us_ok_legislators_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://okhouse.gov/Members'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def get_urls():
    urls = []

    house_members_path = '/Default.aspx'
    scrape_url = base_url + house_members_path
    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    table_rows = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_RadGrid1_ctl00'}).find('tbody').find_all('tr')    
    for tr in table_rows:
        path = tr.find_all('td')[0].find('a').get('href')
        urls.append(base_url + '/' + path)

    return urls

def scrape(url):
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    row = scraper_utils.initialize_row()

    # TODO - Source ID
    # TODO - Most Recent Term ID

    # Source URL
    row.source_url = url

    # TODO - Name (Full, last, first, middle, suffix)
    
    # Party ID + Party
    row.party = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblParty'}).text
    row.party_id = scraper_utils.get_party_id(row.party)

    # TODO - Role
    # TODO - Years active
    # TODO - Committees
    
    # Phone Numbers
    original_phone_number_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblPhone'}).text
    phone_number = {
        'office': 'capital office',
        'number': format_phone_number_str(original_phone_number_str)
    }
    row.phone_numbers.append(phone_number)

    # Addressess
    original_address_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblCapitolRoom'}).find_parent('div').text
    address = {
        'location': 'capitol office',
        'address': format_address_str(original_address_str),
    }
    row.addresses.append(address)

    # print(address)
    # TODO - Email
    # TODO - Birthday
    # TODO - Seniority
    # TODO - Occupation
    # TODO - Education
    # TODO - Military Exp
    # TODO - Areas Served
    # TODO - District

    return row

def format_phone_number_str(original_str):
    phone_number = re.sub('[()]', '', original_str)
    phone_number = re.sub(' ', '-', phone_number)
    return phone_number

def format_address_str(original_str):
    address = original_str.strip()
    address = re.sub(' +', ' ', address)
    address = re.sub('\([0-9]+\) [0-9]{3}-[0-9]{4}|\r|\xa0', '', address)

    # TODO - Refactor this weird bug fix
    address = address.split('\n')

    new_address = [re.sub('\r|\xa0', '', a)
        for a in address]
    
    new_address = ','.join(new_address[:-1])

    new_address = re.sub(', Room, [0-9]{3}|\.', '', new_address)
    new_address = re.sub('OK ', 'OK, ', new_address)

    return new_address

if __name__ == '__main__':
    urls = [get_urls()[0]]
    print(urls)

    # Scrape data from collected URLs serially, which is slower:
    data = [scrape(url) for url in urls]

    pprint(data)
    # print(data)
    # Speed things up using pool.
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    print('Complete!')