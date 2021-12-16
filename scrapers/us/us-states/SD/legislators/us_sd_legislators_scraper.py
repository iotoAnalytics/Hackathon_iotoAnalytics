# Unavailable data - seniority, military_experience
# Wiki data - years_active, birthday, education

import os
import re
import sys

import multiprocessing
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from tqdm import tqdm
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from urllib.request import urlopen as uReq

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'SD'
LEGISLATOR_TABLE_NAME = 'us_sd_legislators'

BASE_URL = 'https://sdlegislature.gov'
LEGISLATORS_PATH = '/Legislators'
WIKI_URL = 'https://en.wikipedia.org'
LEGISLATURE_PATH = '/wiki/South_Dakota_Legislature'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
WIKI_DATA_TO_MERGE = ['years_active', 'birthday', 'education', 'wiki_url']

PARTY_FULL = {
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
    try:
        return session_data['SessionId']
    except:
        pass

def get_most_recent_term_id(session_data):
    try:
        return session_data['Year']
    except:
        pass

def get_members_areas_served(session_id):
    response = scraper_utils.request('https://sdlegislature.gov/api/DistrictCounties')
    scraper_utils.crawl_delay(crawl_delay)

    members_areas_served = {}

    for district_data in response.json():
        if district_data['SessionId'] == session_id:
            members_areas_served[district_data['District']['District'].lstrip('0')] = district_data['District']['Counties']

    return members_areas_served

def get_session_members_data(session_id):
    try:
        response = scraper_utils.request('https://sdlegislature.gov/api/SessionMembers/Session/'
            + str(session_id))
        scraper_utils.crawl_delay(crawl_delay)
        return response.json()
    except:
        pass


def init_most_recent_term_id(member_data, mrti):
    try:
        if not member_data['InactiveDate']:
            member_data['Year'] = mrti
    except:
        pass


def init_areas_served(member_data, members_areas_served):
    try:
        district = member_data['District'].lstrip('0')
        member_data['AreasServed'] = members_areas_served[district]
    except:
        pass


def get_legislators_wiki_urls(wiki_url):
    wiki_urls_with_district = []
    try:
        soup = _create_soup(wiki_url, SOUP_PARSER_TYPE)
        scraper_utils.crawl_delay(crawl_delay)

        for section in soup.select('div[aria-labelledby*="Members_of_"]'):
            container = section.find('div', {'class': 'div-col'})
            ordered_list = container.find(['ol', 'ul'])
            list_items = ordered_list.find_all('li')

            for idx, li in enumerate(list_items, start=1):
                anchors = li.find_all('a')

                for a in anchors:
                    if '/wiki' in (url:= a.get('href')):
                        wiki_item = (str(idx), WIKI_URL + url)
                        wiki_urls_with_district.append(wiki_item)

        return wiki_urls_with_district
    except:
        pass

def scrape_wiki(wiki_item):
    district, wiki_url = wiki_item

    wiki_data = scraper_utils.scrape_wiki_bio(wiki_url)
    wiki_crawl_delay = scraper_utils.get_crawl_delay(WIKI_URL)
    scraper_utils.crawl_delay(wiki_crawl_delay)

    wiki_data['district'] = district

    return wiki_data

def merge_all_wiki_data(legislator_data, wiki_data):
    leg_df = pd.DataFrame(legislator_data)
    leg_df = leg_df.drop(columns = WIKI_DATA_TO_MERGE)
    leg_df['district'] = leg_df['district'].apply(lambda district: re.sub('[^0-9]', '', district))

    wiki_df = pd.DataFrame(wiki_data)[['name_first', 'name_last', 'district', *WIKI_DATA_TO_MERGE]]
    leg_wiki_df = pd.merge(leg_df, wiki_df, how='left', on=['name_first', 'name_last', 'district'])

    for key in WIKI_DATA_TO_MERGE:
        leg_wiki_df[key] = leg_wiki_df[key].replace({np.nan: None})

        if key not in set(['birthday', 'most_recent_term_id']):
            isna = leg_wiki_df[key].isna()
            leg_wiki_df.loc[isna, key] = pd.Series([[]] * isna.sum()).values

    return leg_wiki_df.to_dict('records')

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def set_member_data(member_data):
    row = scraper_utils.initialize_row()

    _set_source_id(row, member_data)
    _set_most_recent_term_id(row, member_data)
    _set_source_url(row, member_data)
    _set_name(row, member_data)
    _set_party(row, member_data)
    _set_role(row, member_data)
    _set_committees(row, member_data)
    _set_phone_numbers(row, member_data)
    _set_addresses(row, member_data)
    _set_email(row, member_data)
    _set_occupation(row, member_data)
    _set_areas_served(row, member_data)
    _set_district(row, member_data)
    _set_gender(row, member_data)

    return row

def _set_source_id(row, member_data):
    try:
        source_id = str(member_data['SessionMemberId'])
        row.source_id = source_id
    except:
        pass

def _set_most_recent_term_id(row, member_data):
    try:
        most_recent_term_id = member_data['Year']
        row.most_recent_term_id = most_recent_term_id
    except:
        pass

def _set_source_url(row, member_data):
    try:
        session_member_id = str(member_data['SessionMemberId'])
        source_url = BASE_URL + LEGISLATORS_PATH + '/Profile/' + session_member_id
        row.source_url = source_url
    except:
        pass

def _set_name(row, member_data):
    try:
        human_name = HumanName(member_data['Name'])
        row.name_first = human_name.first
        row.name_last = human_name.last
        row.name_middle = human_name.middle
        row.name_suffix = human_name.suffix
        row.name_full = human_name.full_name
    except:
        pass

def _set_party(row, member_data):
    try:
        party = PARTY_FULL.get(member_data['Politics'])
        row.party = party
        row.party_id = scraper_utils.get_party_id(party)
    except:
        pass

def _set_role(row, member_data):
    try:
        role = member_data['MemberTypeLong']
        row.role = role
    except:
        pass

def _set_committees(row, member_data):
    try:
        committees = []

        member_id = str(member_data['SessionMemberId'])
        response = scraper_utils.request('https://sdlegislature.gov/api/SessionMembers/Committees/' + member_id)

        committees_data = response.json()['Committees']

        for committee_data in committees_data:
            data = committee_data['SessionCommittees']

            if not data:
                data = committee_data['InterimYearCommittee']

            if not data:
                data = committee_data['ConferenceCommittee']

            committee = {
                'role': data['Description'],
                'committee': data['Name']
            }
            committees.append(committee)

        row.committees = committees
    except:
        pass

def _set_phone_numbers(row, member_data):
    try:
        phone_numbers = []

        _set_phone_number(phone_numbers, 'Home', member_data)
        _set_phone_number(phone_numbers, 'Capitol', member_data)
        _set_phone_number(phone_numbers, 'Business', member_data)

        row.phone_numbers = phone_numbers
    except:
        pass

def _set_phone_number(phone_numbers, office, member_data):
    try:
        if number:= member_data[f'{office}Phone']:
            phone_number = {
                'office': office,
                'number': number
            }
            phone_numbers.append(phone_number)
    except:
        pass

def _set_addresses(row, member_data):
    addresses = []
    try:
        if home_address:= ' '.join([member_data['HomeAddress1'], member_data['HomeCity'],
            member_data['HomeState'], member_data['HomeZip']]):
            address = {
                'location': 'Home',
                'address': home_address
            }
            addresses.append(address)

        row.addresses = addresses
    except:
        pass

def _set_email(row, member_data):
    try:
        email = member_data['EmailState']
        row.email = email
    except:
        pass

def _set_occupation(row, member_data):
    try:
        if not member_data['Occupation']:
            return

        occupation = member_data['Occupation'].split('/')
        row.occupation = occupation
    except:
        pass

def _set_areas_served(row, member_data):
    try:
        areas_served = [area.strip() for area in member_data['AreasServed'].split(',')]
        row.areas_served = areas_served
    except:
        pass

def _set_district(row, member_data):
    try:
        district = member_data['District'].lstrip('0')
        row.district = district
    except:
        pass

def _set_gender(row, member_data):
    try:
        human_name = HumanName(member_data['Name'])
        gender = scraper_utils.get_legislator_gender(human_name.first, human_name.last)
        row.gender = gender
    except:
        pass

def main():
    print('SOUTH DAKOTA!')
    print('Wanna go back to you though your nothing but a town ♫ ♫ ♫')
    print('On the South Dakota grass I lay me down ♫ ♫ ♫')

    print('\nSCRAPING SOUTH DAKOTA LEGISLATORS\n')

    # Get session data and IDs
    print(DEBUG_MODE and 'Initializing...\n' or '', end='')
    session_data = get_current_session_data()
    session_id = get_current_session_id(session_data)
    most_recent_term_id = get_most_recent_term_id(session_data)

    # Get members data
    print(DEBUG_MODE and 'Scraping legislators data...\n' or '', end='')
    members_areas_served = get_members_areas_served(session_id)
    session_members_data = get_session_members_data(session_id)

    # Initialize most_recent_term_id and areas_served into members data
    for member_data in session_members_data:
        if member_data != None:
            init_most_recent_term_id(member_data, most_recent_term_id)
            init_areas_served(member_data, members_areas_served)

    # Set fields
    data = [set_member_data(member_data) for member_data in session_members_data]

    # Collect wiki urls
    print(DEBUG_MODE and 'Collecting wiki URLs...\n' or '', end='')
    wiki_urls = get_legislators_wiki_urls(WIKI_URL + LEGISLATURE_PATH)

    # Scrape data from wiki URLs
    print(DEBUG_MODE and 'Scraping data from wiki URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    # Merge data from wikipedia
    print(DEBUG_MODE and 'Merging wiki data with legislators...\n' or '', end='')
    merged_data = merge_all_wiki_data(data, wiki_data)

    # Special Case - Session 96, Maggie Sutton (4002) is Margaret Sutton in wikipedia. Remove if this is no longer the case
    # Special Case - Session 96, R. Blake Curd (3926) is Blake Curd in wikipedia. Remove if this is no longer the case
    # Special Case - Session 96, Sam Marty (3967) is J. Sam Marty in wikipedia. Remove if this is no longer the case
    for legor in merged_data:
        if legor['source_id'] == '4002' and legor['district'] == '10' and legor['name_last'] == 'Sutton' and legor['name_first'] == 'Maggie':
            legor['wiki_url'] = 'https://en.wikipedia.org/wiki/Margaret_Sutton_(politician)'
        elif legor['source_id'] == '3926' and legor['district'] == '12' and legor['name_last'] == 'Curd' and legor['name_first'] == 'R.':
            legor['wiki_url'] = 'https://en.wikipedia.org/wiki/Blake_Curd'
        elif legor['source_id'] == '3967' and legor['district'] == '28' and legor['name_last'] == 'Marty' and legor['name_first'] == 'Sam':
            legor['wiki_url'] = 'https://en.wikipedia.org/wiki/J._Sam_Marty'

    # Write to database
    if not DEBUG_MODE:
        print('Writing to database...\n')
        scraper_utils.write_data(merged_data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()