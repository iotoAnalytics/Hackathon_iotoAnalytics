# Unavailable data - source_id, seniority, military_experience
# Wiki data - education

import os
import multiprocessing
import re
import requests
import ssl
import sys
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from urllib.request import urlopen as uReq
import numpy as np
import pandas as pd

ssl._create_default_https_context = ssl._create_unverified_context

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'WV'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators'

BASE_URL = 'https://www.wvlegislature.gov'
HOUSE_PATH = '/House'
SENATE_PATH = '/Senate1'
WIKI_URL = 'https://en.wikipedia.org'
LEGISLATURE_PATH = '/wiki/West_Virginia_Legislature'
BALLOTPEDIA_URL = 'https://ballotpedia.org'
BALLOTPEDIA_HOUSE_PATH = '/West_Virginia_House_of_Delegates'
BALLOTPEDIA_SENATE_PATH = '/West_Virginia_State_Senate'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
PEM_PATH = os.path.join('..', 'us_wv.pem')
WIKI_DATA_TO_MERGE = ['education']

# TODO: Update for new legislatures
CURRENT_LEGISLATURE = '85'
LEGISLATURE_START_YEAR = {
    '85': '2021'
}

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


def get_urls():
    urls = []

    roster_path = '/roster.cfm'

    # Get house members
    scrape_url = BASE_URL + HOUSE_PATH + roster_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE, custom_pem=True)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', 'tabborder').find_all('tr', {'valign': 'top'})
    house_urls = [BASE_URL + HOUSE_PATH + '/' + path.find('td').find('a').get('href')
                  for path in table_rows]

    # Get senate members
    scrape_url = BASE_URL + SENATE_PATH + roster_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE, custom_pem=True)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', 'tabborder').find_all('tr', {'valign': 'top'})
    senate_urls = [BASE_URL + SENATE_PATH + '/' + path.find('td').find('a').get('href')
                   for path in table_rows]

    urls = house_urls + senate_urls

    # Remove all vacant urls
    urls = list(filter(lambda url: 'Vacant' not in url, urls))

    return urls


def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE, custom_pem=True)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    _set_most_recent_term_id(row)
    _set_source_url(row, url)
    _set_name(row, soup)
    _set_party(row, soup)
    _set_role(row, url)
    _set_committees(row, soup)
    _set_phone_numbers(row, soup)
    _set_addresses(row, soup)
    _set_email(row, soup)
    _set_areas_served(row, soup)
    _set_district(row, soup)
    _set_biography_fields(row, soup)
    return row


def get_legislators_wiki_urls(wiki_url):
    wiki_urls_with_district = []

    soup = _create_soup(wiki_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    for section in soup.select('div[aria-labelledby*="Members_of_"]'):
        container = section.find('div', {'class': 'div-col'})
        legor_list = container.find(['ol', 'ul'])

        # Find list containing legislators
        if legor_list:
            list_items = legor_list.find_all('li')
        else:
            list_items = container.findChildren('div', recursive=False)

        current_district = 1
        for idx, li in enumerate(list_items, start=1):
            # Get district of legislator depending on list type
            if legor_list:
                current_district = idx
            else:
                text = li.find('div').get_text()
                text = re.sub('[^0-9]', '', text)
                if text.isdigit():
                    current_district = text
            
            anchors = li.find_all('a')
            for a in anchors:
                if '/wiki' in (url:= a.get('href')):
                    wiki_item = (str(current_district), WIKI_URL + url)
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
    leg_df['district'] = leg_df['district'].apply(lambda district: re.sub('[^0-9]', '', district))

    wiki_df = pd.DataFrame(wiki_data)[['name_first', 'name_last', 'district', *WIKI_DATA_TO_MERGE]]
    leg_wiki_df = pd.merge(leg_df, wiki_df, how='left', on=['name_first', 'name_last', 'district'])

    for key in WIKI_DATA_TO_MERGE:
        leg_wiki_df[key] = leg_wiki_df[key].replace({np.nan: None})

        if key not in set(['birthday', 'most_recent_term_id']):
            isna = leg_wiki_df[key].isna()
            leg_wiki_df.loc[isna, key] = pd.Series([[]] * isna.sum()).values

    return leg_wiki_df.to_dict('records')


def get_ballotpedia_data(ballotpedia_url):
    all_ballotpedia_data = []

    soup = _create_soup(ballotpedia_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table = soup.find('table', id='officeholder-table')
    table_rows = table.find('tbody').find_all('tr')

    for tr in table_rows:
        table_cols = tr.find_all('td')
        district = re.search('District ([0-9]+)', table_cols[0].get_text()).group(1)
        
        # Skip if Vacant
        if not (anchor := table_cols[1].find('a')):
            continue
        
        url = anchor.get('href')
        name = anchor.get_text()
        human_name = HumanName(name)

        ballotpedia_data = {
            'name_first': human_name.first,
            'name_last': human_name.last,
            'district': district,
            'wiki_url': url
        }
        all_ballotpedia_data.append(ballotpedia_data)
    
    return all_ballotpedia_data


def merge_all_ballotpedia_data(legislator_data, ballotpedia_data):
    leg_df = pd.DataFrame(legislator_data)
    leg_df = leg_df.drop(columns = 'wiki_url')
    ballotpedia_df = pd.DataFrame(ballotpedia_data)[['name_last', 'district', 'wiki_url']]
    leg_ballotpedia_df = pd.merge(leg_df, ballotpedia_df, how='left', on=['name_last', 'district'])

    # Fix all invalid values
    leg_ballotpedia_df.replace({np.nan: None}, inplace = True)

    return leg_ballotpedia_df.to_dict('records')


def _create_soup(url, soup_parser_type, custom_pem=False):
    if custom_pem:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'
        }
        page = requests.get(url, headers=headers, verify=PEM_PATH)
    else:
        page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup


def _set_most_recent_term_id(row):
    most_recent_term_id = LEGISLATURE_START_YEAR.get(CURRENT_LEGISLATURE)
    row.most_recent_term_id = most_recent_term_id


def _set_source_url(row, url):
    row.source_url = url


def _set_name(row, soup):
    try:
        name_str = soup.find('div', {'id': 'wrapleftcolr'}).find('h2').text
        formatted_name = re.sub('\([A-Za-z0-9\,\s\-]+\)', '', name_str).strip()
        human_name = HumanName(formatted_name)

        row.name_first = human_name.first
        row.name_last = human_name.last
        row.name_middle = human_name.middle
        row.name_suffix = human_name.suffix
        row.name_full = human_name.full_name
    except:
        pass


def _set_party(row, soup):
    party_str = soup.find('div', {'id': 'wrapleftcolr'}).find('h2').text
    try:
        formatted_party = re.search('\((R|D) - ([A-Za-z]+), ([0-9]+)\)', party_str).group(1)

        party = ''
        if formatted_party == 'R':
            party = 'Republican'
        elif formatted_party == 'D':
            party = 'Democrat'

        row.party = party
        row.party_id = scraper_utils.get_party_id(party)
    except:
        pass


def _set_role(row, url):
    role = re.search('\?member=([A-Za-z]+)', url).group(1)
    row.role = role


def _set_committees(row, soup):
    committees = []

    anchors = soup.find('div', {'id': 'wraprightcolr'}).find_all('a')

    # Filter out irrelevant anchors
    committee_anchors = list(filter(lambda anchor: 'committees' in anchor.get('href'), anchors))

    for committee_anchor in committee_anchors:
        # Default role is member unless specified before the anchor tag
        # e.g. MINORITY VICE CHAIR:
        role_str = 'Member'
        if not committee_anchor.previousSibling.isspace():
            role_str = committee_anchor.previousSibling.strip().title()[:-1]

        committee_str = committee_anchor.text.strip()

        committee = {
            'role': role_str,
            'committee': committee_str
        }
        committees.append(committee)

    row.committees = committees


def _set_phone_numbers(row, soup):
    phone_numbers = []
    try:
        location_elements = soup.find('div', {'id': 'wrapleftcolr'}).find_all('i')

        for element in location_elements:
            location_str = _format_phone_location(element.text)
            number_str = _format_phone_number(element.nextSibling)

            phone_number = {
                'office': location_str,
                'number': number_str
            }
            phone_numbers.append(phone_number)

        row.phone_numbers = phone_numbers
    except Exception as e:
        print(e)


def _format_phone_location(phone_location_str):
    phone_location = phone_location_str.replace('Phone:', '')
    phone_location = phone_location.strip()
    return phone_location


def _format_phone_number(phone_number_str):
    phone_number = phone_number_str.strip()
    phone_number = re.sub('[\(\)]', '', phone_number)
    phone_number = phone_number.replace(' ', '-')
    phone_number = phone_number.replace('--', '-')
    return phone_number


def _set_addresses(row, soup):
    addresses = []

    location_elements = soup.find('div', {'id': 'wrapleftcolr'}).find_all('b')

    # Remove non-office elements
    location_elements = list(filter(lambda element: ':' in element.text, location_elements))

    for element in location_elements:
        location_str = _format_address_location(element.text)
        address_str = _format_address(element)
        address = {
            'location': location_str,
            'address': address_str
        }
        addresses.append(address)

    row.addresses = addresses


def _format_address_location(address_location_str):
    # May be in the following formats:
    #   Capital Office:
    #   District:
    address_location = address_location_str.replace(' Office', '')
    address_location = address_location[:-1]
    return address_location


def _format_address(address_element):
    address = ''
    curr_line = address_element.nextSibling

    # Add lines of address details until phone details or biography element
    phone_detail_stop_word = 'Phone:'
    biography_element_stop_word = '<ul class="popup">'
    while phone_detail_stop_word not in str(curr_line) and biography_element_stop_word not in str(curr_line):
        line_str = str(curr_line)

        if not line_str.isspace() and '<br/>' not in line_str:
            address += line_str

        curr_line = curr_line.nextSibling
    else:
        address = address.strip()
        address = address.replace('\n', ', ')
        address = address.replace(', WV', ', WV,')

    return address


def _set_email(row, soup):
    email_str = soup.find('div', {'id': 'wrapleftcolr'}).find('a').text
    email = email_str.strip()
    row.email = email


def _set_areas_served(row, soup):
    areas_served_str = soup.find('div', {'id': 'wraprightcolr'}).find('strong').nextSibling
    areas_served_str = str(areas_served_str).strip()
    areas_served_str = areas_served_str.replace(' (part)', '')
    areas_served = areas_served_str.split(', ')
    row.areas_served = areas_served


def _set_district(row, soup):
    district_str = soup.find('div', {'id': 'wrapleftcolr'}).find('h2').text
    try:
        formatted_district = re.search('\((R|D) - ([A-Za-z]+), ([0-9]+)\)', district_str).group(3)
        district = formatted_district.lstrip('0')
        row.district = district
    except:
        pass


def _set_biography_fields(row, soup):
    # Sets the following fields: years_active, birthday
    biography_content = soup.find('ul', {'class': 'popup'}).find('li').find('div')
    fields_elements = biography_content.find_all('strong')

    # Setup dictionary of fields
    biography_fields = {}
    for element in fields_elements:
        biography_fields[f'{element.text.strip()}'] = element

    most_recent_term_id_key = 'Legislative Service'
    years_active_key = 'Legislative Service'
    birthday_key = 'Born'
    occupation_key = 'Born'

    _set_gender(row, biography_content)

    if years_active_key in biography_fields:
        _set_years_active(row, biography_fields[years_active_key])
    if birthday_key in biography_fields:
        _set_birthday(row, biography_fields[birthday_key])
    try:
        first_key = list(biography_fields)[0]
        _set_occupation(row, biography_fields[first_key])
    except:
        pass


def _set_gender(row, biography_content):
    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, biography_content.text) or 'O'
    row.gender = gender


def _set_occupation(row, element):
    occupation_str = str(element.previousSibling).strip()
    is_fixed = _fix_occupation(row, occupation_str)

    # No need to run rest of code if already fixed or occupation doesn't exist
    if is_fixed or occupation_str == 'None':
        return

    occupation_str = re.sub('\((.*?)\)', '', occupation_str)
    occupation_list = re.split('/|; |, |and ', occupation_str)

    occupation = []
    for job in occupation_list:
        if ' - ' in job or ' – ' in job or ' — ' in job:
            job = re.split(' - | – | — ', job, 1)[0]
        if 'for' in job or 'of' in job or 'with' in job:
            job = re.split(' for | of | with ', job, 1)[0]

        if job:
            occupation.append(job.strip())

    row.occupation = occupation


def _fix_occupation(row, text):
    # NOTE: This method fixes special cases. Currently, this is the simplest effective way to set
    #       occupations for the anomalies

    # Evan Hansen - House - 51
    if 'President, Downstream Strategies' in text:
        row.occupation = ['President of Downstream Strategies']

    # John Mandt Jr. - House - 16
    elif 'Fourth Generation Business Owner' in text:
        row.occupation = ['Business Owner']

    # Chris Phillips - House - 47
    elif 'President, CGP Foods, Inc' in text:
        row.occupation = ['President of CGP Foods Inc']

    # Marty Gearheart - House - 27 
    elif 'Managing Member, Gearheart Enterprises, LLC' in text:
        row.occupation = ['Managing Member of Gearheart Enterprises']

    # Ric Griffith - House - 19
    elif 'Pharmacist/Owner of Griffith and Feil Drug' in text:
        row.occupation = ['Pharmacist', 'Owner of Griffith and Feil Drug']

    # Gary G. Howell - House - 56
    elif 'Small Business Owner/Mail Order Auto Parts' in text:
        row.occupation = ['Small Business Owner of Mail Order Auto Parts']

    # Shannon Kimes - House - 9
    elif 'Owner, Kimes Steel and Rail, Inc' in text:
        row.occupation = ['Owner of Kimes Steel and Rail Inc']

    # Michael T. Azinger - Senate - 3 
    elif 'Manager, Azinger Group' in text:
        row.occupation = ['Manager of Azinger Group']

    else:
        return False

    return True


def _set_years_active(row, element):
    years_active_str = str(element.nextSibling).strip()
    years_active_list = re.findall('[0-9]+-[0-9]+', years_active_str)
    years_active = _unpack_years_range(years_active_list)
    row.years_active = years_active


def _unpack_years_range(years_range):
    # Unpacks a list of year ranges
    # e.g. ['2010-2012', '2016-2018'] -> ['2010, 2011, 2012, 2016, 2017, 2018] 
    formatted_years_active = []
    formatted_years_range = [years_boundary.split('-') for years_boundary in years_range]

    for years_boundary in formatted_years_range:
        for year in range(int(years_boundary[0]), int(years_boundary[1]) + 1):
            if year not in formatted_years_active:
                formatted_years_active.append(year)

    return formatted_years_active


def _set_birthday(row, element):
    try:
        birthday_str = str(element.nextSibling).strip()
        birthday_str = re.search('[A-Za-z]+ [0-9]+, [0-9]+', birthday_str).group(0)
        birthday = datetime.strptime(birthday_str, '%B %d, %Y')
        row.birthday = birthday
    except:
        pass


def _get_legislator_row(legislator_data, name_full):
    for row in legislator_data:
        if name_full == row.name_full:
            return row

    return None


def main():
    print('WEST VIRGINIA!')
    print('Country roads, take me home ♫ ♫ ♫')
    print('To the place I belong ♫ ♫ ♫')
    print('West Virginia, mountain mama ♫ ♫ ♫')
    print('Take me home, country roads ♫ ♫ ♫')

    print('\nSCRAPING WEST VIRGINIA LEGISLATORS\n')

    # Collect legislators urls
    print(DEBUG_MODE and 'Collecting legislator URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))
    # data = [scrape(url) for url in urls]

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
    
    # Merge ballotpedia data for wiki_url
    print(DEBUG_MODE and 'Merging ballotpedia data with legislators...\n' or '', end='')
    ballotpedia_house_data = get_ballotpedia_data(BALLOTPEDIA_URL + BALLOTPEDIA_HOUSE_PATH)
    ballotpedia_senate_data = get_ballotpedia_data(BALLOTPEDIA_URL + BALLOTPEDIA_SENATE_PATH)
    all_ballotpedia_data = ballotpedia_house_data + ballotpedia_senate_data
    merged_data = merge_all_ballotpedia_data(merged_data, all_ballotpedia_data)

    # Write to database
    if not DEBUG_MODE:
        print('Writing to database...\n', end='')
        scraper_utils.write_data(merged_data)

    print('\nCOMPLETE!\n')


if __name__ == '__main__':
    main()
