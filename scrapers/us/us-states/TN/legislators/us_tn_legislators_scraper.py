# Unavailable data - source_id, seniority, military_experience
# Wiki data - years_active, education, occupation

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
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'TN'
LEGISLATOR_TABLE_NAME = 'us_tn_legislators'

BASE_URL = 'https://www.capitol.tn.gov/'
HOUSE_PATH = 'house/members/'
SENATE_PATH = 'senate/members/'
WIKI_URL = 'https://en.wikipedia.org'
WIKI_HOUSE_PATH = '/wiki/Tennessee_House_of_Representatives'
WIKI_SENATE_PATH = '/wiki/Tennessee_Senate'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
WIKI_DATA_TO_MERGE = ['years_active', 'education', 'occupation']

# Update for new assemblies
CURRENT_ASSEMBLY = '112'
ASSEMBLY_START_YEAR = {
    '112': '2021',
    '111': '2019',
}

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    legislator_info_idx = 1
    
    # Get house URLs
    scrape_url = BASE_URL + HOUSE_PATH
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'class': 'box tan'}).find('tbody').find_all('tr')
    house_urls = [BASE_URL + HOUSE_PATH + table_row.find_all('td')[legislator_info_idx].find('a').get('href')
        for table_row in table_rows]

    # Get senate URLs
    scrape_url = BASE_URL + SENATE_PATH
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'class': 'box tan'}).find('tbody').find_all('tr')
    senate_urls = [BASE_URL + SENATE_PATH + table_row.find_all('td')[legislator_info_idx].find('a').get('href')
        for table_row in table_rows]

    urls = house_urls + senate_urls

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    try:
        _set_most_recent_term_id(row)
        _set_source_url(row, url)
        _set_name(row, soup)
        _set_party(row, soup)
        _set_role(row, soup)
        _set_committees(row, soup)
        _set_phone_numbers(row, soup)
        _set_addresses(row, soup)
        _set_email(row, soup)
        _set_birthday(row, soup)
        _set_areas_served(row, soup)
        _set_district(row, soup)
    except:
        print(f'Error with: {url}')

    return row

def get_legislators_wiki_urls(main_wiki_url):
    wiki_urls_with_district = []

    soup = _create_soup(main_wiki_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'class': 'sortable wikitable'}).find('tbody').find_all('tr')
    for row in table_rows[1:]:
        fields = row.find_all('td')
        district = fields[0].text.strip()
        path = fields[1].find('a')

        if path:
            wiki_item = (f'{district}', WIKI_URL + path.get('href'))
            wiki_urls_with_district.append(wiki_item)

    return wiki_urls_with_district
        
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

    wiki_df = pd.DataFrame(wiki_data)[['name_first', 'name_last', 'district', *WIKI_DATA_TO_MERGE]]
    leg_wiki_df = pd.merge(leg_df, wiki_df, how='left', on=['name_first', 'name_last', 'district']) 
    
    for key in WIKI_DATA_TO_MERGE:
        leg_wiki_df[key] = leg_wiki_df[key].replace({np.nan: None})

        if key not in set(['birthday', 'most_recent_term_id']):
            isna = leg_wiki_df[key].isna()
            leg_wiki_df.loc[isna, key] = pd.Series([[]] * isna.sum()).values
            
    return leg_wiki_df.to_dict('records')    

def fix_odditites(legislators_data):
    # Fix occupation formats for oddities
    for data in legislators_data:
        _fix_occupation_format(data)

def _create_soup(url, soup_parser_type):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_most_recent_term_id(row):
    most_recent_term_id = ASSEMBLY_START_YEAR.get(CURRENT_ASSEMBLY)
    row.most_recent_term_id = most_recent_term_id

def _set_source_url(row, url):
    row.source_url = url

def _set_name(row, soup):
    name_str = soup.select('body > div.row.content > div > h1')[0].text.strip()
    formatted_name = name_str.split(' ', 1)[1]
    human_name = HumanName(formatted_name)

    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, soup):
    party_str = (
        soup.select('body > div.row.content > div > div > div.box.black.description > div.one > '
            'strong')[0].text.strip()
    )

    row.party = party_str
    row.party_id = scraper_utils.get_party_id(party_str)

def _set_role(row, soup):
    role_str = soup.select('body > div.row.content > div > h1')[0].text.strip()
    role = role_str.split(' ', 1)[0]

    # Fix special abbreviations
    if role == 'Lt.':
        role = 'Governor'
    elif role == 'Rep.':
        role = 'Representative'

    row.role = role

def _set_committees(row, soup):
    committees = []
    
    try:
        committees_list = (
            soup.select('body > div.row.content > div > div > div.padded-25 > div > '
                'div.large-18.medium-18.columns > ul.tabs-container.box.tan')[0]
                .find('li', {'data-tab': 'committees'})
                .find_all('li')
        )

        for committee in committees_list:
            committee_info = committee.text.strip().split(', ')
            committee_item = {
                'role': committee_info[0],
                'committee': committee_info[1]
            }
            committees.append(committee_item)
    except:
        pass

    row.committees = committees

def _set_phone_numbers(row, soup):
    phone_numbers = []

    phone_number_element = (
        soup.select('body > div.row.content > div > div > div.padded-25 > div > '
            'div.large-6.medium-6.columns.side')[0]
            .find('div', {'data-mobilehide': 'contact'})
    )

    office_str = phone_number_element.find('h2').text
    office = _format_location(office_str)

    phone_number_str = phone_number_element.find('p').text.strip()
    search = re.search('Phone:? \(?([0-9]{3})\)?\s?\-?([0-9]{3}-[0-9]{4})', phone_number_str)
    number = search.group(1) + '-' + search.group(2)

    phone_number = {
        'office': office,
        'number': number
    }
    phone_numbers.append(phone_number)

    row.phone_numbers = phone_numbers

def _set_addresses(row, soup):
    addresses = []

    address_element = (
        soup.select('body > div.row.content > div > div > div.padded-25 > div > '
            'div.large-6.medium-6.columns.side')[0]
            .find('div', {'data-mobilehide': 'contact'})
            .find_all(lambda tag: tag.name == 'h2' and 'Address' in tag.text)
    )

    for el in address_element:
        location = _format_location(el.text)
        address = _format_address(el.next_element.next_element.next_element.text)

        address_item = {
            'location': location,
            'address': address
        }
        addresses.append(address_item)

    row.addresses = addresses

def _format_location(text):
    location = text.strip().split()[0]
    
    if location == 'Nashville':
        location = 'Capitol'
    
    return location

def _format_address(text):
    address_str = re.sub('[\r\n]+', '', text)
    address_str = re.sub('\s{2,}', ' ', address_str)
    address_str = re.sub('(Phone|Fax) \([0-9]{3}\) [0-9]{3}-[0-9]{4}', '', address_str)
    address = address_str.strip()
    return address

def _set_email(row, soup):
    email_str = (
        soup.select('body > div.row.content > div > div > div.box.black.description > div.photo > '
            'p > a.button.email.icon-mail')[0]
            .get('href')
    )
    email = email_str.replace('mailto:', '')
    row.email = email

def _set_birthday(row, soup):
    birthday_str = (
        soup.select('body > div.row.content > div > div > div.padded-25 > div > '
            'div.large-18.medium-18.columns > ul.tabs-container.box.tan > li:nth-child(1) > ul > '
            'li:nth-child(1)')[0]
            .text
    )

    if 'Born' in birthday_str:
        row.birthday = _format_birthdate(birthday_str)

def _format_birthdate(text):
    birthdate_str = re.sub('(st|nd|rd|th)', '', text)
    birthdate_str = re.sub('Born (on)?', '', birthdate_str)
    
    if search:= re.search('([A-Za-z]+) [0-9]+(\, [0-9]{4})?', birthdate_str):
        birthdate_str = search.group(0).strip()
        month_str = search.group(1)

        # Fix website's typo
        if month_str and month_str == 'Augu':
            birthdate_str = birthdate_str.replace('Augu', 'August')

    birthdate_str = birthdate_str.strip()

    # Match format March 1, 2021
    try:
        birthdate = datetime.strptime(birthdate_str, '%B %d, %Y')
        return birthdate
    except ValueError:
        pass

    # Match format March 1
    try:
        birthdate = datetime.strptime(birthdate_str, '%B %d')
        return birthdate
    except ValueError:
        pass

    # Match format March 2021
    try:
        birthdate = datetime.strptime(birthdate_str, '%B %Y')
        return birthdate
    except ValueError:
        print(f'Cannot set birthdate: {birthdate_str}')

def _set_areas_served(row, soup):
    areas_served_str = (
        soup.select('body > div.row.content > div > div > div.box.black.description > div.two > '
            'br')[0]
            .next_element
    )

    areas_served = _format_areas_served(areas_served_str)
    row.areas_served = areas_served

def _format_areas_served(text):
    areas_served_str = re.sub('[\r\n]+', '', text)
    areas_served_str = re.sub('(and|[Pp]art of|Count(ies|y))', '', areas_served_str)
    areas_served_str = re.sub('\s{2,}', ' ', areas_served_str)
    areas_served_str = areas_served_str.strip()

    areas_served = re.split(', | ', areas_served_str)
    return areas_served

def _set_district(row, soup):
    district_str = (
        soup.select('body > div.row.content > div > div > div.box.black.description > '
            'div.two > strong')[0]
            .text
    )

    district = district_str.split()[-1]
    row.district = district

def _fix_occupation_format(legislator_data):
    if not legislator_data['occupation']:
        return

    formatted_occupation = []

    # Janice Bowling - Senate - 16
    if legislator_data['occupation'][0] == 'EducationCongressional Staffer':
        legislator_data['occupation'] = ['Education', 'Congressional Staffer']

    # Mark Pody - Senate - 17
    elif legislator_data['occupation'][0] == 'PoliticianInsurance Producer':
        legislator_data['occupation'] = ['Politician', 'Insurance Producer']

    # Format occupations that are not formatted properly
    for job in legislator_data['occupation']:
        occupation_str = re.sub('\[[0-9]+\]|\([A-Za-z]+\)', '', job)
        occupations = re.split('/| , | and ', occupation_str)
        occupations = [occupation.strip() for occupation in occupations]
        formatted_occupation += occupations

    legislator_data['occupation'] = formatted_occupation

def main():
    print('TENNESSEE!')
    print('O Tennessee: Fair Tennessee: ♫ ♫ ♫')
    print('Our love for thee can never die: ♫ ♫ ♫')
    print('Dear homeland, Tennessee ♫ ♫ ♫\n')

    print('\nSCRAPING TENNESSEE LEGISLATORS\n')

    # Collect legislators urls
    print(DEBUG_MODE and 'Collecting legislator URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from legislator URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))

    # Collect wiki urls
    print(DEBUG_MODE and 'Collecting wiki URLs...\n' or '', end='')
    wiki_urls = get_legislators_wiki_urls(WIKI_URL + WIKI_HOUSE_PATH) + \
        get_legislators_wiki_urls(WIKI_URL + WIKI_SENATE_PATH)

    # Scrape data from wiki URLs
    print(DEBUG_MODE and 'Scraping data from wiki URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    # Merge data from wikipedia
    print(DEBUG_MODE and 'Merging wiki data with legislators...\n' or '', end='')
    merged_data = merge_all_wiki_data(data, wiki_data)
    
    # Fix oddities
    print(DEBUG_MODE and 'Fixing formats in oddities...\n' or '', end='')
    fix_odditites(merged_data)

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(merged_data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()