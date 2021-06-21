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

import requests

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

def get_session_members_json(session_number):
    response = scraper_utils.request('https://sdlegislature.gov/api/SessionMembers/Session/' + session_number)
    return response.json()

def set_json_data(json_data):
    row = scraper_utils.initialize_row()

    # source_id
    _set_source_id(row, json_data)

    # most_recent_term_id
    # _set_most_recent_term_id(row, json[])

    # source_url
    _set_source_url(row, json_data)

    # name (full, last, first, middle, suffix)
    _set_name(row, json_data)

    # party_id & party
    _set_party(row, json_data)

    # role
    _set_role(row, json_data)

    # TODO - years_active

    # committees
    _set_committees(row, json_data)

    # phone_number
    _set_phone_numbers(row, json_data)

    # addresses
    _set_addresses(row, json_data)

    # email
    _set_email(row, json_data)

    # TODO - birthday
    # TODO - seniority

    # occupation
    _set_occupation(row, json_data)

    # TODO - education
    # TODO - military_experience

    # areas_served
    _set_areas_served(row, json_data)

    # district
    _set_district(row, json_data)

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

def _set_source_id(row, json_data):
    source_id = json_data['SessionMemberId']
    row.source_id = source_id

def _set_most_recent_term_id(row, soup):
    pass

def _set_source_url(row, json_data):
    session_member_id = str(json_data['SessionMemberId'])
    source_url = BASE_URL + LEGISLATORS_PATH + '/Profile/' + session_member_id
    row.source_url = source_url
    print(source_url)

def _set_name(row, json_data):
    human_name = HumanName(json_data['Name'])
    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, json_data):
    party = FULL_PARTY.get(json_data['Politics'])
    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

FULL_PARTY = {
    'R': 'Republican',
    'D': 'Democrat'
}

def _set_role(row, json_data):
    role = json_data['MemberTypeLong']
    row.role = role

def _set_committees(row, json_data):
    committees = []

    member_id = str(json_data['SessionMemberId'])
    response = scraper_utils.request('https://sdlegislature.gov/api/SessionMembers/Committees/' + member_id)
    
    committees_data = response.json()['Committees']

    for committee_data in committees_data:
        committee_data = committee_data['SessionCommittees']
        committee = {
            'role': committee_data['Description'],
            'committee': committee_data['Name']
        }
        committees.append(committee)
    
    row.committees = committees

def _set_phone_numbers(row, json_data):
    phone_numbers = []

    _set_phone_number(phone_numbers, 'Home', json_data)
    _set_phone_number(phone_numbers, 'Capitol', json_data)
    _set_phone_number(phone_numbers, 'Business', json_data)
    
    row.phone_numbers = phone_numbers

def _set_phone_number(phone_numbers, office, json_data):
    if number:= json_data[f'{office}Phone']:
        phone_number = {
            'office': office,
            'number': number
        }
        phone_numbers.append(phone_number)

def _set_addresses(row, json_data):
    addresses = []

    home_address = ' '.join([json_data['HomeAddress1'], json_data['HomeCity'], json_data['HomeState'], json_data['HomeZip']])
    if home_address:
        address = {
            'location': 'Home',
            'address': home_address
        }
        addresses.append(home_address)
    
    row.addresses = addresses

def _set_email(row, json_data):
    email = json_data['EmailState']
    row.email = email

def _set_occupation(row, json_data):
    occupation = str(json_data['Occupation']).split('/')
    row.occupation = occupation

def _set_areas_served(row, json_data):
    areas_served = []

    district = json_data['District']
    response = scraper_utils.request('https://sdlegislature.gov/api/DistrictCounties')
    
    for data in response.json():
        if data['SessionId'] == 44 and data['District']['District'] == district:
            areas_served = data['District']['Counties'].split(', ')
            break

    row.areas_served = areas_served
    
def _set_district(row, json_data):
    district = json_data['District']
    row.district = district

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATORS\n')

    session_members_json = get_session_members_json('44')
    data = [set_json_data(member_json) for member_json in session_members_json]

    pprint(data, width=200)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()