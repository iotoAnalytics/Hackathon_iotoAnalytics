# TODO - Special case for HB2002 with Effect May 27, 2021: https://www.wvlegislature.gov/Bill_Status/Bills_history.cfm?input=2002&year=2021&sessiontype=RS&btype=bill
# Unavailable data - source_id, committees, cosponsors, cosponsors_id, votes

import os
import re
import sys
import unicodedata

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

from scraper_utils import USStateLegislationScraperUtils
from us_wv_legislation_utils import SESSIONS, BILL_NAME_ABRV, CHAMBERS, BILL_TEXT_VERSIONS, get_sponsor_role_from_url

DEBUG_MODE = False

STATE_ABBREVIATION = 'WV'
DATABASE_TABLE_NAME = 'us_wv_legislation_test'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators_test'

BASE_URL = 'https://www.wvlegislature.gov/'
BILL_STATUS_PATH = 'Bill_Status/'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
PEM_PATH = os.path.join('..', 'us_wv.pem')

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    bills_path = 'Bills_all_bills.cfm?year=2021&sessiontype=rs&btype=bill'

    # Get bills
    scrape_url = BASE_URL + BILL_STATUS_PATH + bills_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'class': 'tabborder'}).find_all('a')
    urls = [BASE_URL + BILL_STATUS_PATH + path.get('href')
        for path in table_rows if 'Bills_history' in path.get('href')]

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    _set_bill_name(row, soup)
    _set_session(row, soup)
    _set_date_introduced(row, soup)
    _set_source_url(row, url)
    _set_chamber_origin(row, soup)
    _set_bill_type(row, soup)
    _set_bill_title(row, soup)
    _set_current_status(row, soup)
    _set_principal_sponsor(row, soup)
    _set_sponsors(row, soup)
    _set_bill_text(row, soup)
    _set_bill_summary(row, soup)
    _set_actions(row, soup)
    _set_source_topic(row, soup)
    _set_goverlytics_id(row, soup)

    return row

def _create_soup(url, soup_parser_type):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'}
    page = requests.get(url, headers=headers, verify=PEM_PATH)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _get_table_row(soup, field_name):
    table_rows = soup.find('div', {'id': 'bhistcontent'}).find('table').find_all('tr')

    field_lookup = {
        'bill_summary': 'SUMMARY',
        'principal_sponsor': 'LEAD SPONSOR',
        'sponsors': 'SPONSORS',
        'bill_text': 'TEXT',
        'bill_title': 'SUMMARY',
        'source_topic': 'SUBJECT'
    }

    for table_row in table_rows:
        # Check if field exists as a table row
        field = table_row.find('td')
        if field and field_lookup[field_name] in field.text:
            return table_row
    
    return None 

def _set_bill_name(row, soup):
    bill_name_str = soup.find('div', {'id': 'bhistcontent'}).findChild().text
    bill_name_str = bill_name_str.strip()
    
    search = re.search('([A-Za-z\s]+)([0-9]+)', bill_name_str)
    bill_type = search.group(1).strip()
    bill_number = search.group(2)

    bill_name = BILL_NAME_ABRV.get(bill_type) + bill_number
    row.bill_name = bill_name

def _set_session(row, soup):
    session_title = soup.find('div', {'id': 'bhistcontent'}).find('table').find('strong')
    session_str = str(session_title.nextSibling).strip()
    
    search = re.search('([0-9]+)\(([A-Z]+)\)', session_str)
    session_year = search.group(1)
    session_type = search.group(2)
    
    session = session_year + ' ' + SESSIONS.get(session_type)
    row.session = session

def _set_date_introduced(row, soup):
    date_introduced_row = soup.find('table', {'class': 'tabborder'}).find_all('tr')[-1]
    date_introduced_idx = 2
    date_introduced_str = date_introduced_row.find_all('td')[date_introduced_idx].text
    date_introduced = datetime.strptime(date_introduced_str, '%m/%d/%y')
    row.date_introduced = date_introduced

def _set_source_url(row, url):
    row.source_url = url

def _set_chamber_origin(row, soup):
    chamber_origin_row = soup.find('table', {'class': 'tabborder'}).find_all('tr')[-1]
    chamber_origin_idx = 0
    chamber_origin_str = chamber_origin_row.find_all('td')[chamber_origin_idx].text
    chamber_origin = CHAMBERS.get(chamber_origin_str)
    row.chamber_origin = chamber_origin

def _set_bill_type(row, soup):
    bill_type_str = soup.find('div', {'id': 'bhistcontent'}).findChild().text
    bill_type_str = bill_type_str.strip()
    
    if 'Bill' in bill_type_str:
        row.bill_type = 'Bill'
    elif 'Resolution' in bill_type_str:
        row.bill_type = 'Resolution'

def _set_bill_title(row, soup):
    bill_title_row = _get_table_row(soup, 'bill_title')
    bill_title_str_idx = 1 
    bill_title = bill_title_row.find_all('td')[bill_title_str_idx].text.strip()
    row.bill_title = bill_title
    
def _set_current_status(row, soup):
    table_rows = soup.find('table', {'class': 'tabborder'}).find_all('tr')
    current_status_idx = 1
    current_status_row = table_rows[current_status_idx]
    
    # 1 column means action is "Effective Ninety Days from Passage" 
    if len(current_status_row.find_all('td')) == 1:
        current_status_str = current_status_row.find('td').text.strip()
        
        # Only get action, not including its date
        search = re.search('[a-zA-Z]+(\s[a-zA-Z]+)*', current_status_str)
        current_status = search.group(0)
    else:
        description_idx = 1
        current_status = current_status_row.find_all('td')[description_idx].text.strip()
    
    row.current_status = current_status

# FIXME - Refactor this method
def _set_principal_sponsor(row, soup):
    principal_sponsor_row = _get_table_row(soup, 'principal_sponsor')
    principal_sponsor_element = principal_sponsor_row.find('a')
    
    if not principal_sponsor_element:
        principal_sponsor_idx = 1
        principal_sponsor_element = principal_sponsor_row.find_all('td')[principal_sponsor_idx]
        principal_sponsor_name = principal_sponsor_element.text.strip()
        principal_sponsor_formatted_name = _format_sponsor_name(principal_sponsor_name)
        principal_sponsor = principal_sponsor_formatted_name['name']
        row.principal_sponsor = principal_sponsor

        principal_sponsor_id = scraper_utils.get_legislator_id(name_last=principal_sponsor_formatted_name['name_last'])
        row.principal_sponsor_id = principal_sponsor_id
    else:
        # Name
        principal_sponsor_name = principal_sponsor_element.text.strip()
        principal_sponsor_formatted_name = _format_sponsor_name(principal_sponsor_name)
        principal_sponsor = principal_sponsor_formatted_name['name']
        row.principal_sponsor = principal_sponsor

        # ID
        legislator_url = principal_sponsor_element.get('href')
        principal_sponsor_role = get_sponsor_role_from_url(legislator_url)
        principal_sponsor_id = _get_legislator_id(principal_sponsor_formatted_name, principal_sponsor_role)
        row.principal_sponsor_id = principal_sponsor_id

# NOTE:XXX - Possible error if sponsors are not links
def _set_sponsors(row, soup):
    sponsors_row = _get_table_row(soup, 'sponsors')

    sponsors = []
    sponsors_id = []

    sponsors_list = sponsors_row.find_all('a')
    if sponsors_list:
        for sponsor_element in sponsors_list:
            legislator_url = sponsor_element.get('href')
            sponsor_name = _format_sponsor_name(sponsor_element.text)
            sponsor_role = get_sponsor_role_from_url(legislator_url)
            sponsor_id = _get_legislator_id(sponsor_name, sponsor_role)

            sponsors.append(sponsor_name['name'])
            sponsors_id.append(sponsor_id)
    else:
        # NOTE - Probably shouldn't store sponsors that are likely to have duplicates.
        # TODO - Research whether both senator and delegate can sponsor same bill (ANS: NO)
        sponsors_idx = 1
        sponsors_str = sponsors_row.find_all('td')[sponsors_idx].text.strip()
        sponsors_list = sponsors_str.split(', ')
        
        for sponsor_member in sponsors_list:
            sponsor_name = _format_sponsor_name(sponsor_member)
            sponsor_id = _get_legislator_id(sponsor_name)

            sponsors.append(sponsor_name['name'])
            sponsors_id.append(sponsor_id)

    row.sponsors = sponsors
    row.sponsors_id = sponsors_id

def _format_sponsor_name(name):
    # Remove special roles: (Mr. Speaker) 
    name = re.sub('\s\([A-Za-z\.\s]+\)', '', name)
    
    # Capture group last name and first name
    search = re.search('([A-Za-z]+), ([A-Za-z]+).', name)
    return {
        'name_last': search.group(1) if search else name,
        'name_first': search.group(2) if search else None,
        'name': name
    }

# FIXME - Refactor this method
def _get_legislator_id(name, role=None):
    search_query = {}

    if name['name_last']:
        search_query['name_last'] = name['name_last']
    if role:
        search_query['role'] = role

    if name['name_first']:
        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', name['name_first'], **search_query)
    else:
        gov_id = scraper_utils.get_legislator_id(**search_query)

    return gov_id

def _set_bill_text(row, soup):
    # Also sets bill_description

    bill_text_row = _get_table_row(soup, 'bill_text') 

    # Get HTML links of bill versions
    bill_text_link_element_idx = 1 
    bill_text_link_elements = bill_text_row.find_all('td')[bill_text_link_element_idx].find_all('a')
    bill_text_link_elements = list(filter(lambda link: 'HTML' in link.get('title'), bill_text_link_elements))
    
    # Get most recent bill
    most_recent_bill = _get_most_recent_bill(bill_text_link_elements)
    
    # Return if no bill exists
    if not most_recent_bill:
        return

    # Bill text
    bill_url = most_recent_bill['url']
    bill_text = _get_bill_text_from_html(bill_url)
    row.bill_text = bill_text

    # Bill description
    bill_description = most_recent_bill['description']
    row.bill_description = bill_description

def _get_most_recent_bill(bill_text_link_elements):
    """Returns the most recent version of the bill.
    
    Args:
        bill_text_link_elements (list): List of bill text link soup elements.

    Returns:
        {
            'url' (str): Link to the HTML bill text
            'description' (str): Bill version
        }
    """
    for version in BILL_TEXT_VERSIONS:
        for element in bill_text_link_elements:
            if version in element.get('title'):
                return {
                    'description': element.get('title').replace('HTML - ', ''),
                    'url': BASE_URL + BILL_STATUS_PATH + element.get('href')
                }

    # Return first link if not any of existing bill text version
    if len(bill_text_link_elements):
        first_link_element = bill_text_link_elements[0]
        return {
            'description': first_link_element.get('title').replace('HTML - ', ''),
            'url': BASE_URL + BILL_STATUS_PATH + first_link_element.get('href')
        }
    
    return None

def _get_bill_text_from_html(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    bill_text = soup.find('div', {'class': 'textcontainer'}).text
    bill_text = unicodedata.normalize('NFKD', bill_text)
    return bill_text

def _set_bill_summary(row, soup):
    bill_summary_row = _get_table_row(soup, 'bill_summary') 
    bill_summary_idx = 1
    bill_summary = bill_summary_row.find_all('td')[bill_summary_idx].text.strip()
    row.bill_summary = bill_summary

def _set_actions(row, soup):
    # Skip table header
    table_rows = soup.find('table', {'class': 'tabborder'}).find_all('tr')[1:]
    
    actions = [_get_action_info_(table_row) for table_row in table_rows]
    row.actions = actions

def _get_action_info_(action_row):
    fields_length = len(action_row.find_all('td'))

    # 1 column means action is in the following string format:
    if fields_length == 1:
        action_info = _get_action_element_from_string_row(action_row)
    else:
        action_info = _get_action_info_from_table_row(action_row)

    return action_info

def _get_action_element_from_string_row(action_row):
    # e.g. Effective Ninety Days from Passage - (July 9, 2021)
    action_row_str = action_row.text
    search = re.search('([a-zA-Z]+(\s[a-zA-Z]+)*) - \(([A-Za-z0-9\,\s]+)\)', action_row_str)

    description_idx, date_idx = 1, 3
    description = search.group(description_idx).strip()
    date_str = search.group(date_idx)
    date = datetime.strptime(date_str, '%B %d, %Y')

    return {
        'date': date,
        'description': description
    }

def _get_action_info_from_table_row(action_row):
    chamber_idx, description_idx, date_idx = 0, 1, 2

    fields = action_row.find_all('td')

    chamber_str = fields[chamber_idx].text
    chamber = CHAMBERS.get(chamber_str)

    description = fields[description_idx].text.strip()

    date_str = fields[date_idx].text
    date = datetime.strptime(date_str, '%m/%d/%y')
    
    return {
        'date': date,
        'action_by': chamber,
        'description': description
    }

# TODO - What to do with more than one topic
def _set_source_topic(row, soup):
    try:
        source_topic_row = _get_table_row(soup, 'source_topic')
        source_topic_str_idx = 1 
        source_topic_elements = source_topic_row.find_all('td')[source_topic_str_idx].find_all('a')

        source_topic_list = [element.text for element in source_topic_elements]
        source_topic = ', '.join(source_topic_list)

        row.source_topic = source_topic
    except:
        # print('Cannot set source topic')
        pass

def _set_goverlytics_id(row, soup):
    session = _format_session_for_gov_id(soup)
    goverlytics_id = STATE_ABBREVIATION + '_' + session + '_' + row.bill_name
    row.goverlytics_id = goverlytics_id

def _format_session_for_gov_id(soup):
    session_title = soup.find('div', {'id': 'bhistcontent'}).find('table').find('strong')
    session_str = str(session_title.nextSibling).strip()
    session = re.sub('[\(\)\s]', '', session_str)
    return session

def main():
    print('\nSCRAPING WEST VIRGINIA LEGISLATION\n')

    # Collect legislation urls
    print(DEBUG_MODE and 'Collecting legislation URLs...\n' or '', end='')
    urls = get_urls()
    
    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    with Pool(NUM_POOL_PROCESSES) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')
    
if __name__ == '__main__':
    main()