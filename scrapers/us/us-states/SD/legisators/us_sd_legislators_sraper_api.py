# Unavailable data - military_experience
# Wiki data - years_active, birthday, seniority, education

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

DEBUG_MODE = True

STATE_ABBREVIATION = 'SD'
LEGISLATOR_TABLE_NAME = 'us_sd_legislators_test'

BASE_URL = 'https://sdlegislature.gov'
LEGISLATORS_PATH = '/Legislators'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)

FULL_PARTY = {
    'R': 'Republican',
    'D': 'Democrat'
}

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_current_session_data():
    response = scraper_utils.request('https://sdlegislature.gov/api/Sessions/')
    scraper_utils.crawl_delay(crawl_delay)
    
    for session in response.json():
        if session['CurrentSession']:
            return session

def get_current_session_id(session_data):
    return session_data['SessionId']

def get_most_recent_term_id(session_data):
    return session_data['Year']

def get_members_areas_served(session_id):
    response = scraper_utils.request('https://sdlegislature.gov/api/DistrictCounties')
    scraper_utils.crawl_delay(crawl_delay)

    members_areas_served = {}

    for district_data in response.json():
        if district_data['SessionId'] == session_id:
            members_areas_served[district_data['District']['District']] = district_data['District']['Counties']

    return members_areas_served

def get_session_members_data(session_id):
    response = scraper_utils.request('https://sdlegislature.gov/api/SessionMembers/Session/' 
        + str(session_id))
    scraper_utils.crawl_delay(crawl_delay)
    return response.json()

def init_most_recent_term_id(member_data, mrti):
    if not member_data['InactiveDate']:
        member_data['Year'] = mrti

def init_areas_served(member_data, members_areas_served):
    district = member_data['District']
    member_data['AreasServed'] = members_areas_served[district]

def _set_source_id(row, member_data):
    source_id = str(member_data['SessionMemberId'])
    row.source_id = source_id
        
def _set_most_recent_term_id(row, member_data):
    most_recent_term_id = member_data['Year']
    row.most_recent_term_id = most_recent_term_id

def _set_source_url(row, member_data):
    session_member_id = str(member_data['SessionMemberId'])
    source_url = BASE_URL + LEGISLATORS_PATH + '/Profile/' + session_member_id
    row.source_url = source_url

def _set_name(row, member_data):
    human_name = HumanName(member_data['Name'])
    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, member_data):
    party = FULL_PARTY.get(member_data['Politics'])
    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

def _set_role(row, member_data):
    role = member_data['MemberTypeLong']
    row.role = role

def _set_committees(row, member_data):
    committees = []

    member_id = str(member_data['SessionMemberId'])
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

def _set_phone_numbers(row, member_data):
    phone_numbers = []

    _set_phone_number(phone_numbers, 'Home', member_data)
    _set_phone_number(phone_numbers, 'Capitol', member_data)
    _set_phone_number(phone_numbers, 'Business', member_data)
    
    row.phone_numbers = phone_numbers

def _set_phone_number(phone_numbers, office, member_data):
    if number:= member_data[f'{office}Phone']:
        phone_number = {
            'office': office,
            'number': number
        }
        phone_numbers.append(phone_number)

def _set_addresses(row, member_data):
    addresses = []

    if home_address:= ' '.join([member_data['HomeAddress1'], member_data['HomeCity'],
        member_data['HomeState'], member_data['HomeZip']]):
        address = {
            'location': 'Home',
            'address': home_address
        }
        addresses.append(home_address)
    
    row.addresses = addresses

def _set_email(row, member_data):
    email = member_data['EmailState']
    row.email = email

def _set_occupation(row, member_data):
    occupation = member_data['Occupation'].split('/')
    row.occupation = occupation

def _set_areas_served(row, member_data):
    areas_served = member_data['AreasServed'].split(',')
    row.areas_served = areas_served
    
def _set_district(row, member_data):
    district = member_data['District']
    row.district = district

def main():
    print('\nSCRAPING SOUTH DAKOTA LEGISLATORS\n')

    # Get session data and IDs
    session_data = get_current_session_data()
    session_id = get_current_session_id(session_data)
    most_recent_term_id = get_most_recent_term_id(session_data)
    
    # Get members data
    members_areas_served = get_members_areas_served(session_id)
    members_committees = get_members_committees(session_id)
    session_members_data = get_session_members_data(session_id)

    # Initialize most_recent_term_id and areas_served into members data
    for member_data in session_members_data:
        init_most_recent_term_id(member_data, most_recent_term_id)
        init_areas_served(member_data, members_areas_served)

    # Set fields
    data = [set_member_data(member_data) for member_data in session_members_data]

    # Wikipedia
    pprint(data, width=200)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()