# Unavailable data - source_id, seniority, military_experience
# Wiki data - education, occupation,

import os
import re
import sys

import multiprocessing
import requests
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

STATE_ABBREVIATION = 'WV'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators'

BASE_URL = 'https://www.wvlegislature.gov'
HOUSE_PATH = '/House'
SENATE_PATH = '/Senate1'
WIKI_URL = 'https://en.wikipedia.org'
WIKI_HOUSE_PATH = '/wiki/West_Virginia_House_of_Delegates'
WIKI_SENATE_PATH = '/wiki/West_Virginia_Senate'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
PEM_PATH = os.path.join('..', 'us_wv.pem')

# Update for new legislatures
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
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', 'tabborder').find_all('tr', {'valign': 'top'})
    house_urls = [BASE_URL + HOUSE_PATH + '/' + path.find('td').find('a').get('href')
        for path in table_rows]

    # Get senate members
    scrape_url = BASE_URL + SENATE_PATH + roster_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', 'tabborder').find_all('tr', {'valign': 'top'})
    senate_urls = [BASE_URL + SENATE_PATH + '/' + path.find('td').find('a').get('href')
        for path in table_rows]

    urls = house_urls + senate_urls

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
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

def get_wiki_urls():
    wiki_urls = []
    
    wiki_member_list_url = [WIKI_URL + WIKI_HOUSE_PATH, WIKI_URL + WIKI_SENATE_PATH]

    for url in wiki_member_list_url: 
        page = scraper_utils.request(url)
        soup = BeautifulSoup(page.content, SOUP_PARSER_TYPE)
        scraper_utils.crawl_delay(crawl_delay)

        table_rows = soup.find('table', {'class': 'sortable wikitable'}).find('tbody').find_all('tr')

        for row in table_rows[1:]:
            try:
                path = row.find('span', {'class': 'fn'}).find('a').get('href')
                wiki_urls.append(WIKI_URL + path)
            except Exception:
                pass

    return wiki_urls
    
def merge_all_wiki_data(legislator_data, wiki_urls):
    with Pool(NUM_POOL_PROCESSES) as pool:
        wiki_data = list(tqdm(pool.imap(_scrape_wiki, wiki_urls)))

    for data in wiki_data:
        _merge_wiki_data(legislator_data, data, most_recent_term_id=False, years_active=False, birthday=False, occupation=False)

def _scrape_wiki(url):
    wiki_data = scraper_utils.scrape_wiki_bio(url)
    wiki_crawl_delay = scraper_utils.get_crawl_delay(WIKI_URL)
    scraper_utils.crawl_delay(wiki_crawl_delay)
    return wiki_data

def _create_soup(url, soup_parser_type):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'}
    page = requests.get(url, headers=headers, verify=PEM_PATH)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_most_recent_term_id(row):
    most_recent_term_id = LEGISLATURE_START_YEAR.get(CURRENT_LEGISLATURE)
    row.most_recent_term_id = most_recent_term_id

def _set_source_url(row, url):
    row.source_url = url

def _set_name(row, soup):
    name_str = soup.find('div', {'id': 'wrapleftcolr'}).find('h2').text
    formatted_name = re.sub('\([A-Za-z0-9\,\s\-]+\)', '', name_str).strip()
    human_name = HumanName(formatted_name)

    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, soup):
    party_str = soup.find('div', {'id': 'wrapleftcolr'}).find('h2').text
    formatted_party = re.search('\((R|D) - ([A-Za-z]+), ([0-9]+)\)', party_str).group(1)

    party = ''
    if formatted_party == 'R':
        party = 'Republican'
    elif formatted_party == 'D':
        party = 'Democrat'

    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

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

def _format_phone_location(phone_location_str):
    phone_location = phone_location_str.replace('Phone:', '')
    phone_location = phone_location.strip()
    return phone_location

def _format_phone_number(phone_number_str):
    phone_number = phone_number_str.strip()
    phone_number = re.sub('[\(\)]', '', phone_number)
    phone_number = phone_number.replace(' ', '-')
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
    formatted_district = re.search('\((R|D) - ([A-Za-z]+), ([0-9]+)\)', district_str).group(3)
    district = formatted_district.lstrip('0')
    row.district = district

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

    if years_active_key in biography_fields:
        _set_years_active(row, biography_fields[years_active_key])
    if birthday_key in biography_fields:
        _set_birthday(row, biography_fields[birthday_key])
    
    first_key = list(biography_fields)[0]
    _set_occupation(row, biography_fields[first_key])

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
        row.occupation = ['Pharmacist','Owner of Griffith and Feil Drug']

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
    except Exception:
        pass

def _merge_wiki_data(legislator_data, wiki_data, birthday=True, education=True, occupation=True,
                    years_active=True, most_recent_term_id=True):
    full_name = wiki_data['name_first'] + ' ' + wiki_data['name_last']

    legislator_row = _get_legislator_row(legislator_data, full_name)

    if not legislator_row:
        return

    for bio_info in wiki_data:
        if birthday:
            legislator_row.birthday = wiki_data['birthday']
        if education:
            legislator_row.education = wiki_data['education']
        if occupation:
            legislator_row.occupation = wiki_data['occupation']
        if years_active:
            legislator_row.years_active = wiki_data['years_active']
        if most_recent_term_id:
            legislator_row.most_recent_term_id = wiki_data['most_recent_term_id']
 
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

    # Collect wiki urls
    print(DEBUG_MODE and 'Collecting wiki URLs...\n' or '', end='')
    wiki_urls = get_wiki_urls()

    # Merge data from wikipedia
    print(DEBUG_MODE and 'Merging wiki data with house legislators...\n' or '', end='')
    merge_all_wiki_data(data, wiki_urls)

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')
    
if __name__ == '__main__':
    main()