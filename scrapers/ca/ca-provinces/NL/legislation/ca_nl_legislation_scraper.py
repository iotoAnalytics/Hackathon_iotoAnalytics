# Unavailable data: source_id, date_introduced, committees, bill_type, sponsors, sponsors_id, cosponsors, cosponsors_id, bill_description, votes, source_topic

import os
import re
import sys
import unicodedata
from tqdm import tqdm

from bs4 import BeautifulSoup
from multiprocessing import Pool
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from datetime import datetime

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import CAProvinceTerrLegislationScraperUtils

DEBUG_MODE = False

PROV_ABBREVIATION = 'NL'
DATABASE_TABLE_NAME = 'ca_nl_legislation'
LEGISLATOR_TABLE_NAME = 'ca_nl_legislators'

BASE_URL = 'https://assembly.nl.ca/'
SOUP_PARSER_TYPE = 'lxml'

scraper_utils = CAProvinceTerrLegislationScraperUtils(PROV_ABBREVIATION, DATABASE_TABLE_NAME, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    urls = []

    bills_path = 'housebusiness/bills/'
    url = BASE_URL + bills_path
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    # Get sessions from latest assembly
    content = soup.find_all('div', {'class': 'container'})[3]
    sessions = content.find('ul').find_all('li')
    
    # Get urls from sessions of latest assembly
    for session in sessions:
        session_path = session.find('a').get('href')
        urls.append(BASE_URL + bills_path + session_path)

    return urls

def scrape(url):
    data = []

    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'class': 'bills'}).find_all('tr')

    # Filter out rows with no URL to actual Bill
    table_rows = list(filter(lambda row: row.find('a'), table_rows))

    for table_row in tqdm(table_rows):
        row = scraper_utils.initialize_row()

        # Bill No, Title, First Reading, Second Reading, Committee, Amendments, Third Reading, Royal Assent, Act 
        fields = table_row.find_all('td')
        
        # Get bill text in list form
        source_url = url + fields[1].find('a').get('href')
        bill_soup = _create_soup(source_url, SOUP_PARSER_TYPE)
        scraper_utils.crawl_delay(crawl_delay)
        bill_text_list = _get_bill_text_from_soup(bill_soup)

        _set_bill_name(row, fields)
        _set_session(row, soup)
        _set_source_url(row, source_url)
        _set_chamber_origin(row)
        _set_bill_title(row, fields)
        _set_current_status(row, fields)
        _set_principal_sponsor(row, bill_text_list)
        _set_bill_text(row, bill_text_list)
        _set_bill_summary(row, bill_text_list)
        _set_actions(row, fields)
        _set_region(row)
        _set_goverlytics_id(row)

        data.append(row)
    
    return data

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_bill_name(row, soup):
    bill_no_idx = 0
    bill_no_str = soup[bill_no_idx].text.replace('.', '')
    bill_name = 'BILL' + bill_no_str
    row.bill_name = bill_name

def _set_session(row, soup):
    content = soup.find_all('div', {'class': 'container'})[3]
    session = content.find('p', {'class': 'text-center'}).text
    session = _format_session_str(session)
    row.session = session

def _set_date_introduced(row, text):
    date_introduced = datetime.strptime(text, '%b. %d, %Y')
    row.date_introduced = date_introduced

def _set_source_url(row, url):
    row.source_url = url

def _set_chamber_origin(row):
    row.chamber_origin = 'House of Assembly'

def _set_bill_title(row, soup):
    bill_title_idx = 1
    bill_title_str = soup[bill_title_idx].text
    bill_title = re.sub('\r|\n|\t', '', bill_title_str)
    row.bill_title = bill_title

def _set_current_status(row, fields):
    # Bill No, Title, First Reading, Second Reading, Committee, Amendments, Third Reading, Royal Assent, Act        
    first_reading_date = _remove_nonbreak_space(fields[2].text)
    second_reading_date = _remove_nonbreak_space(fields[3].text)
    committee_reading_date = _remove_nonbreak_space(fields[4].text)
    third_reading_date = _remove_nonbreak_space(fields[6].text)
    royal_assent_date = _remove_nonbreak_space(fields[7].text)

    current_status = ''

    if not first_reading_date:
        current_status = 'First Reading'
    elif not second_reading_date:
        current_status = 'Second Reading'
    elif not committee_reading_date:
        current_status = 'Committee Reading'
    elif not third_reading_date:
        current_status = 'Third Reading'
    else:
        current_status = 'Royal Assent'

    row.current_status = current_status

def _set_principal_sponsor(row, bill_text_list):
    for line in bill_text_list:
        if 'HONOURABLE' in line or 'Honourable' in line:
            sponsor = re.sub('HONOURABLE|Honourable|,\s[A-Za-z\.]+', '', line)
            sponsor = re.sub('\s{2,}', ' ', sponsor)
            sponsor = sponsor.strip().title()
            row.principal_sponsor = sponsor

            gov_id = scraper_utils.get_legislator_id(name_full = sponsor)
            row.principal_sponsor_id = gov_id
            return

def _set_actions(row, fields):
    # Bill No, Title, First Reading, Second Reading, Committee, Amendments, Third Reading, Royal Assent, Act        
    first_reading_date = _remove_nonbreak_space(fields[2].text)
    second_reading_date = _remove_nonbreak_space(fields[3].text)
    committee_reading_date = _remove_nonbreak_space(fields[4].text)
    third_reading_date = _remove_nonbreak_space(fields[6].text)
    royal_assent_date = _remove_nonbreak_space(fields[7].text)

    actions = []

    action = {
        'date': '',
        'action_by': 'House of Assembly',
        'description': ''
    }

    if first_reading_date:
        action['date'] = datetime.strptime(first_reading_date, '%b. %d, %Y')
        action['description'] = 'First Reading'
        actions.append(action)

    if second_reading_date:
        action['date'] = datetime.strptime(second_reading_date, '%b. %d, %Y')
        action['description'] = 'Second Reading'
        actions.append(action)

    if committee_reading_date:
        action['date'] = datetime.strptime(committee_reading_date, '%b. %d, %Y')
        action['description'] = 'Committee Reading'
        actions.append(action)

    if third_reading_date:
        action['date'] = datetime.strptime(third_reading_date, '%b. %d, %Y')
        action['description'] = 'Third Reading'
        actions.append(action)

    if royal_assent_date:
        # Fix typo
        if royal_assent_date == 'Ar. 23, 2021':
            royal_assent_date = 'Apr. 23, 2021'

        action['date'] = datetime.strptime(royal_assent_date, '%b. %d, %Y')
        action['description'] = 'Royal Assent'
        actions.append(action)

    row.actions = actions

def _set_bill_text(row, bill_text_list):
    bill_text = _format_bill_text_list(bill_text_list)
    row.bill_text = bill_text

def _set_bill_summary(row, bill_text_list):
    for line in bill_text_list:
        if 'AN ACT' in line:
            bill_summary = line.replace('   ', ' ')
            row.bill_summary = bill_summary
            return

def _set_region(row):
    region = scraper_utils.get_region(PROV_ABBREVIATION)
    row.region = region

def _set_goverlytics_id(row):
    goverlytics_id = row.province_territory + '_' + row.session + '_' + row.bill_name
    row.goverlytics_id = goverlytics_id

def _format_session_str(text):
    # 50th GENERAL ASSEMBLY FIRST SESSION -> 50-1
    general_assembly_no = re.search('[0-9]+', text).group(0)
    formatted_session = general_assembly_no + '-'

    if 'FIRST' in text:
        formatted_session += '1'
    elif 'SECOND' in text:
        formatted_session += '2'
    elif 'THIRD' in text:
        formatted_session += '3'
    elif 'FOURTH' in text:
        formatted_session += '4'
    elif 'FIFTH' in text:
        formatted_session += '5'

    return formatted_session

def _remove_nonbreak_space(text):
    nonbreak_space = u'\xa0'
    text = text.replace(nonbreak_space, '')
    return text

def _get_bill_text_from_soup(soup):
    text_list_soup = soup.find('div', {'class': 'Section1'}).find_all('p')
    text_list = _normalize_text(text_list_soup)

    return text_list

def _normalize_text(text_list_soup):
    # Normalizes .HTM text for parsing
    text_list = [unicodedata.normalize('NFKD', text.text) for text in text_list_soup]
    text_list = [text.replace('\r\n', ' ') for text in text_list]
    return text_list

def _format_bill_text_list(bill_text_list):
    # Formats a bill text from list to text form
    bill_text = ''

    for line in bill_text_list:
        bill_text += line + '\n'

    return bill_text.strip()

def main():
    print('\nSCRAPING NEWFOUNDLAND AND LABRADOR LEGISLATIONS\n')

    # Collect urls of session for latest assembly
    print(DEBUG_MODE and 'Collecting all sessions for latest NL assembly...\n' or '', end='')
    urls = get_urls()

    # Scrape urls
    print(DEBUG_MODE and 'Begin scraping each urls...\n' or '', end='')
    data = []
    for url in urls:
        data.extend(scrape(url))

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if not DEBUG_MODE:
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()