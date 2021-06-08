# Unavailable data - source_id, cosponsors, cosponsors_id, votes

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

import us_wv_legislation_utils as wv_utils
from scraper_utils import USStateLegislationScraperUtils

DEBUG_MODE = False

STATE_ABBREVIATION = 'WV'
DATABASE_TABLE_NAME = 'us_wv_legislation'
LEGISLATOR_TABLE_NAME = 'us_wv_legislators'

BASE_URL = 'https://www.wvlegislature.gov/'
BILL_STATUS_PATH = 'Bill_Status/'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
PEM_PATH = os.path.join('..', 'us_wv.pem')
CURRENT_YEAR = datetime.now().year

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    urls = []
    bills_urls = _get_bills_urls()

    for url in bills_urls:
        try:
            soup = _create_soup(url, SOUP_PARSER_TYPE)
            scraper_utils.crawl_delay(crawl_delay)

            # Skip table header
            table_rows = soup.find('table', {'class': 'tabborder'}).find_all('tr')[1:]
            session_urls = [BASE_URL + BILL_STATUS_PATH + path.find('a').get('href')
                for path in table_rows]
            urls.extend(session_urls)
        except:
            print(f'WARNING: {url} is not scraped because it doesn\'t exists yet')

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    try:
        _set_bill_name(row, soup)
        _set_session(row, url)
        _set_date_introduced(row, soup)
        _set_source_url(row, url)
        _set_chamber_origin(row, soup)
        _set_committees(row, soup)
        _set_bill_type(row, url)
        _set_bill_title(row, soup)
        _set_current_status(row, soup)
        _set_principal_sponsor(row, soup)
        _set_sponsors(row, soup)
        _set_bill_text(row, soup)
        _set_bill_summary(row, soup)
        _set_actions(row, soup)
        _set_source_topic(row, soup)
        _set_goverlytics_id(row, url)
    except:
        print(f'Could not scrape: {url}')

    return row

def _get_bills_urls():
    bills_urls = []

    # Bill
    for bill_session in wv_utils.SESSIONS_FULL:
        path = f'Bills_all_bills.cfm?year={CURRENT_YEAR}&sessiontype={bill_session}&btype=bill'
        bills_urls.append(BASE_URL + BILL_STATUS_PATH + path)

    # Resolution
    for bill_session in wv_utils.SESSIONS_FULL:
        path = f'res_list.cfm?year={CURRENT_YEAR}&sessiontype={bill_session}&btype=res'
        bills_urls.append(BASE_URL + BILL_STATUS_PATH + path)

    return bills_urls

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

    bill_name = wv_utils.BILL_NAME_ABRV.get(bill_type) + bill_number
    row.bill_name = bill_name

def _set_session(row, url):
    if search:= re.search('&?year=([0-9]+)&sessiontype=([a-zA-Z0-9]+)', url):
        session_year = search.group(1)
        session_type = search.group(2)
    
        session = session_year + ' ' + wv_utils.SESSIONS_FULL.get(session_type)
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
    chamber_origin = wv_utils.CHAMBERS_FULL.get(chamber_origin_str)
    row.chamber_origin = chamber_origin

def _set_committees(row, soup):
    links = soup.find('table', {'class': 'tabborder'}).find_all('a')
    
    committees = []
    committee_urls = set()

    for link in links:
        url = link.get('href')
        search = re.search('&abbvar=([A-Z]+)&chamvar=(H|S)', url)

        # Check if search result exists and is not already added to list of committees
        if search and url not in committee_urls:
            committee_urls.add(url)
            committee_abvr = search.group(1)
            committee_chamber = search.group(2)

            bill_committee = {
                'chamber': wv_utils.CHAMBERS_FULL.get(committee_chamber),
                'committee': wv_utils.COMMITTEE_FULL.get(committee_abvr)
            }
            committees.append(bill_committee)
        
    row.committees = committees

def _set_bill_type(row, url):
    if search:= re.search('&btype=([a-zA-Z]+)', url):
        bill_type_abvr = search.group(1)
        bill_type = wv_utils.BILL_TYPE_FULL.get(bill_type_abvr)
        row.bill_type = bill_type

def _set_bill_title(row, soup):
    bill_title_row = _get_table_row(soup, 'bill_title')
    bill_title_str_idx = 1 
    bill_title = bill_title_row.find_all('td')[bill_title_str_idx].text.strip()
    row.bill_title = bill_title
    
def _set_current_status(row, soup):
    table_rows = soup.find('table', {'class': 'tabborder'}).find_all('tr')
    current_status_idx = 1
    current_status_row = table_rows[current_status_idx]

    # Format is one of the following: 
    # 1 column: Effective Ninety Days from Passage - (July 7, 2021)
    # 3 columns: Senate Finance, Committee, 04/05/21
    if len(current_status_row.find_all('td')) == 1:
        current_status_str = current_status_row.find('td').text.strip()
        current_status = current_status_str
    else:
        description_idx = 1
        current_status = current_status_row.find_all('td')[description_idx].text.strip()
    
    row.current_status = current_status

def _set_principal_sponsor(row, soup):
    sponsor_row = _get_table_row(soup, 'principal_sponsor')
    sponsors_list = _get_sponsors(soup, sponsor_row)

    if sponsors_list:
        principal_sponsor = sponsors_list[0]['name']
        principal_sponsor_id = sponsors_list[0]['id']

        row.principal_sponsor = principal_sponsor
        row.principal_sponsor_id = principal_sponsor_id

def _set_sponsors(row, soup):
    sponsor_row = _get_table_row(soup, 'sponsors')
    sponsors_list = _get_sponsors(soup, sponsor_row)

    sponsors = [sponsor['name'] for sponsor in sponsors_list]
    sponsors_id = [sponsor['id'] for sponsor in sponsors_list]

    row.sponsors = sponsors
    row.sponsors_id = sponsors_id

def _get_sponsors(soup, sponsor_row):
    sponsors_list = []

    sponsors_idx = 1
    sponsors_element = sponsor_row.find_all('td')[sponsors_idx]
    
    # Split sponsors into a list of sponsors
    sponsors_str_list = _get_sponsors_list(sponsors_element)

    # Return if no sponsors exists
    if sponsors_str_list[0] == '':
        return sponsors_list

    # Get role of sponsors
    bill_name_str = soup.find('div', {'id': 'bhistcontent'}).findChild().text
    if 'House' in bill_name_str:
        sponsor_role = 'Delegate'
    elif 'Senate' in bill_name_str:
        sponsor_role = 'Senator'

    # Get sponsor name and id
    for sponsor in sponsors_str_list:
        formatted_sponsor = _get_sponsor_info_from_name(sponsor)

        # Initialize default
        role = sponsor_role

        # Change role if sponsor is Speaker
        if formatted_sponsor['role']:
            role = formatted_sponsor['role']

        sponsor_name = formatted_sponsor['name']
        sponsor_id = _get_legislator_id(formatted_sponsor, role)
        
        sponsors_list.append({
            'name': sponsor_name,
            'id': sponsor_id
        })

    return sponsors_list

def _get_sponsors_list(sponsors_element):
    sponsors_str_list = ['']

    if sponsors_element:
        sponsors_str_list = sponsors_element.text.strip()
        sponsors_str_list = sponsors_str_list.replace('\r\n', '')
        sponsors_str_list = re.sub(',(\s[A-Za-z]+\.)', r'\1', sponsors_str_list)
        sponsors_str_list = re.sub('\s{2,}', ' ', sponsors_str_list)
        sponsors_str_list = sponsors_str_list.split(', ')

    return sponsors_str_list

def _get_sponsor_info_from_name(name):
    # Set role for Speaker
    role = 'Speaker' if 'Speaker' in name else None

    # Remove special roles: (Mr. Speaker) or (Mr. President)
    name = re.sub('\s\([A-Za-z\.\s]+\)', '', name)

    # Capture group last name and first name
    search = re.search('([A-Za-z]+) ([A-Za-z]+).', name)

    return {
        'name_last': search.group(1) if search else name,
        'starts_with': search.group(2) if search else None,
        'name': name,
        'role': role
    }

def _get_legislator_id(name, role):
    search_query = {}

    # Set up search query
    if name['name_last']:
        search_query['name_last'] = name['name_last']
    if role:
        search_query['role'] = role
    
    if name['starts_with']:
        starts_with = name['starts_with']
        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', starts_with, **search_query)
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
    for version in wv_utils.BILL_TEXT_VERSIONS:
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
    
    actions = [_get_action_info(table_row) for table_row in table_rows if _get_action_info(table_row)]
    row.actions = actions

def _get_action_info(action_row):
    fields_length = len(action_row.find_all('td'))

    # 1 column means action is in the following string format:
    if fields_length == 1:
        action_info = _get_action_element_from_string_row(action_row)
    else:
        action_info = _get_action_info_from_table_row(action_row)

    return action_info

def _get_action_element_from_string_row(action_row):
    # Format is one of the following:
    # Effective Ninety Days from Passage - (July 9, 2021)
    # Effective July 1, 2021
    action_row_str = action_row.text

    if search:= re.search('[a-zA-Z]+\s[0-9]{1,2}\,\s[0-9]{4}', action_row_str):
        description = action_row_str.strip()
        date_str = search.group(0)
        date = datetime.strptime(date_str, '%B %d, %Y')

        return {
            'date': date,
            'description': description
        }

def _get_action_info_from_table_row(action_row):
    chamber_idx, description_idx, date_idx = 0, 1, 2
    fields = action_row.find_all('td')

    chamber_str = fields[chamber_idx].text
    chamber = wv_utils.CHAMBERS_FULL.get(chamber_str)

    description = fields[description_idx].text.strip()

    date_str = fields[date_idx].text
    date = datetime.strptime(date_str, '%m/%d/%y')
    
    return {
        'date': date,
        'action_by': chamber,
        'description': description
    }

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

def _set_goverlytics_id(row, url):
    session = _format_session_for_gov_id(url)
    goverlytics_id = STATE_ABBREVIATION + '_' + session + '_' + row.bill_name
    row.goverlytics_id = goverlytics_id

def _format_session_for_gov_id(url):
    if search:= re.search('&?year=([0-9]+)&sessiontype=([a-zA-Z0-9]+)', url):
        session_year = search.group(1)
        session_type = search.group(2)

        session = session_year + session_type
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