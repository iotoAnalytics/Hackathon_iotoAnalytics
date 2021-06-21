# Unavailable data - 
# Wiki data - 

import os
import re
import sys

import multiprocessing
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils

# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By

DEBUG_MODE = True

STATE_ABBREVIATION = 'SD'
LEGISLATOR_TABLE_NAME = 'us_sd_legislators'

BASE_URL = 'https://sdlegislature.gov'
LEGISLATORS_PATH = '/Legislators'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
WEBDRIVER_PATH = os.path.join('..', '..', '..', '..', '..', 'web_drivers', 'chrome_win_90.0.4430.24', 'chromedriver.exe')

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    legislators_listing_path = '/Listing/44'

    soup = _create_soup_from_selenium(BASE_URL + LEGISLATORS_PATH + legislators_listing_path)
    scraper_utils.crawl_delay(crawl_delay)

    legislators_links = (soup.select('#scrolling-techniques > main > div > div > '
        'div.col-sm-12.col-md-10.col > div > div > div.v-data-table.elevation-1.theme--light > '
        'div > table > tbody')[0]
        .find_all('a'))

    urls = [BASE_URL + legislators_link.get('href')
        for legislators_link in legislators_links]

    return urls

def scrape(url):
    soup = _create_soup_from_selenium(url)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()
    print(url)
    # source_id
    _set_source_id(row, url)

    # most_recent_term_id
    _set_most_recent_term_id(row, soup)

    # source_url
    _set_source_url(row, url)

    # name (full, last, first, middle, suffix)
    _set_name(row, soup)

    # party_id & party
    _set_party(row, soup)

    # role
    _set_role(row, soup)

    # TODO - years_active

    # committees
    _set_committees(row, url)

    # phone_number
    _set_phone_number(row, soup)

    # addresses
    _set_addresses(row, soup)

    # email
    _set_email(row, soup)

    # TODO - birthday
    # TODO - seniority

    # occupation
    _set_occupation(row, soup)

    # TODO - education
    # TODO - military_experience

    # areas_served
    _set_areas_served(row, soup)

    # district
    _set_district(row, soup)

    return row

def _create_soup_from_selenium(url, soup_parser_type='lxml'):
    options = Options()
    options.headless = True

    driver = webdriver.Chrome(WEBDRIVER_PATH, options=options)
    driver.switch_to.default_content()
    driver.get(url)
    driver.maximize_window()

    sleep(5)

    html = driver.page_source
    soup = BeautifulSoup(html, soup_parser_type)
    driver.quit()

    return soup

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_source_id(row, url):
    source_id = re.search('Profile\/([0-9]+)', url).group(1)
    row.source_id = source_id

def _set_most_recent_term_id(row, soup):
    try:
        mrti_idx = 1
        most_recent_term_id = (soup.select('#scrolling-techniques > main > div > div > '
            'div.col-sm-12.col-md-10.col > div > div.v-card__title > span')[mrti_idx]
            .text)
        row.most_recent_term_id = most_recent_term_id
    except IndexError:
        print(soup)

def _set_source_url(row, url):
    row.source_url = url

def _set_name(row, soup):
    name_str = (soup.select('#scrolling-techniques > main > div > div > '
        'div.col-sm-12.col-md-10.col > div > div.v-card__title')[0]
        .text)
    name_str = ' '.join(name_str.split(' ')[1:-1])
    name_str = name_str.replace('-', '').strip()
    
    human_name = HumanName(name_str)
    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, soup):
    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})

    party_element = biography_soup.find(lambda tag: tag.name == 'b' and 'Party' in tag.text) 
    party = party_element.nextSibling.text
    
    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

def _set_role(row, soup):
    role_idx = 0
    role = (soup.select('#scrolling-techniques > main > div > div > '
        'div.col-sm-12.col-md-10.col > div > div.v-card__title > span')[role_idx]
        .text.strip())
    row.role = role

# TODO - Refactor code
def _set_committees(row, url):
    committees = []
    
    soup = _create_soup_from_selenium(url + '/Committees')

    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})

    if not biography_soup:
        print(url)
        print("Didn't work")

    committee_elements = biography_soup.find_all('table')
    committee_elements_rows = [element.find('tbody').find_all('tr') for element in committee_elements]
    committee_name_idx, role_idx = 0, 1
    
    for committee_element in committee_elements_rows:
        for element in committee_element:
            committee_fields = element.find_all('td')
            committee = {
                'committee': _format_committee_name(committee_fields[committee_name_idx].text),
                'role': committee_fields[role_idx].text
            }
            committees.append(committee)

    row.committees = committees

def _format_committee_name(text):
    formatted_committee_name = re.sub('\s{2,}', ' ', text)
    formatted_committee_name = formatted_committee_name.strip()
    return formatted_committee_name

def _set_phone_number(row, soup):
    phone_numbers = []

    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})

    offices = ['Home', 'Capitol']

    for office in offices:
        phone_element = biography_soup.find(lambda tag: tag.name == 'b' and office + ':' in tag.text) 
        if phone_element:
            phone_number = {
                'office': office,
                'number': phone_element.findNext().text 
            }
            phone_numbers.append(phone_number)
    
    row.phone_numbers = phone_numbers

def _set_addresses(row, soup):
    addresses = []

    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})

    locations = ['Home']

    for location in locations:
        address_element = biography_soup.find(lambda tag: tag.name == 'b' and location + ' Address:' in tag.text) 
        if address_element:
            address = {
                'location': location,
                'address': address_element.findNext().text + ' ' + address_element.findNext().findNext().text
            }
            addresses.append(address)

    row.addresses = addresses

def _set_email(row, soup):
    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})
    
    email_element = biography_soup.find(lambda tag: tag.name == 'a' and '@sdlegislature.gov' in tag.text) 
    email = email_element.text
    row.email = email

def _set_occupation(row, soup):
    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})
    
    occupation_element = biography_soup.find(lambda tag: tag.name == 'b' and 'Occupation' in tag.text) 
    occupation = str(occupation_element.nextSibling).split('/')
    row.occupation = occupation

def _set_areas_served(row, soup):
    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})
    
    areas_served_element = biography_soup.find(lambda tag: tag.name == 'b' and 'Counties' in tag.text) 
    areas_served = str(areas_served_element.nextSibling).split(',')
    row.areas_served = areas_served

def _set_district(row, soup):
    session_member_id = row.source_id
    biography_soup = soup.find('div', {'sessionmemberid': session_member_id})
    
    district_element = biography_soup.find(lambda tag: tag.name == 'b' and 'District' in tag.text) 
    district = str(district_element.nextSibling).strip()
    row.district = district

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATORS\n')

    # Collect legislators urls
    print(DEBUG_MODE and 'Collecting legislator URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from legislator URLs...\n' or '', end='')
    # with Pool(NUM_POOL_PROCESSES) as pool:
    #     data = list(tqdm(pool.imap(scrape, urls)))
    data = [scrape(url) for url in urls]
    # data = [scrape('https://sdlegislature.gov/Legislators/Profile/1766')]
    # data = [scrape('https://sdlegislature.gov/Legislators/Profile/1768')]

    pprint(data, width=200)

    # Write to database
    # if not DEBUG_MODE:
    # print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    # scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()