# Unavailable data: source_id, date_introduced, committees, bill_type, sponsors, sponsors_id, cosponsors, cosponsors_id, bill_description, votes, source_topic

from pathlib import Path
import os
import sys

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import CAProvinceTerrLegislationScraperUtils
from bs4 import BeautifulSoup
from multiprocessing import Pool
from pprint import pprint
from nameparser import HumanName
import re
from datetime import datetime
from tqdm import tqdm
import unicodedata

BASE_URL = 'https://assembly.nl.ca/'
SOUP_PARSER_TYPE = 'lxml'

PROV_ABBREVIATION = 'NL'
DATABASE_TABLE_NAME = 'ca_nl_legislation_test'
LEGISLATOR_TABLE_NAME = 'ca_nl_legislators_test'

DEBUG_MODE = False

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
        path = fields[1].find('a').get('href')
        source_url = url + path
        bill_soup = _create_soup(source_url, SOUP_PARSER_TYPE)
        scraper_utils.crawl_delay(crawl_delay)
        bill_text_list = _get_bill_text_from_soup(bill_soup)

        # bill_name
        _set_bill_name(row, fields[0])
        
        # session
        _set_session(row, soup)
        
        # source_url
        _set_source_url(row, source_url)

        # chamber_origin
        _set_chamber_origin(row)

        # bill_title
        _set_bill_title(row, fields[1])

        # current_status
        _set_current_status(row, fields)

        # principal_sponsor + principal_sponsor_id
        _set_principal_sponsor(row, bill_text_list)

        # bill_text
        # _set_bill_text(row, bill_text_list)

        # bill_summary
        _set_bill_summary(row, bill_text_list)

        # actions
        _set_actions(row, fields)

        # region
        _set_region(row)

        # goverlytics_id
        _set_goverlytics_id(row)

        data.append(row)
    
    return data

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_bill_name(row, soup):
    bill_no = soup.text.replace('.', '')
    bill_name = 'BILL' + bill_no
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
    bill_title = re.sub('\r|\n|\t', '', soup.text)
    row.bill_title = bill_title

def _set_current_status(row, fields):
    # Bill No, Title, First Reading, Second Reading, Committee, Amendments, Third Reading, Royal Assent, Act        
    first_reading_date = _remove_non_break_space(fields[2].text)
    second_reading_date = _remove_non_break_space(fields[3].text)
    committee_reading_date = _remove_non_break_space(fields[4].text)
    third_reading_date = _remove_non_break_space(fields[6].text)
    royal_assent_date = _remove_non_break_space(fields[7].text)

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
    first_reading_date = _remove_non_break_space(fields[2].text)
    second_reading_date = _remove_non_break_space(fields[3].text)
    committee_reading_date = _remove_non_break_space(fields[4].text)
    third_reading_date = _remove_non_break_space(fields[6].text)
    royal_assent_date = _remove_non_break_space(fields[7].text)

    actions = []

    if first_reading_date:
        action = {
            'date': datetime.strptime(first_reading_date, '%b. %d, %Y'),
            'action_by': 'House of Assembly',
            'description': 'First Reading'
        }
        actions.append(action)

    if second_reading_date:
        action = {
            'date': datetime.strptime(second_reading_date, '%b. %d, %Y'),
            'action_by': 'House of Assembly',
            'description': 'Second Reading'
        }
        actions.append(action)

    if committee_reading_date:
        action = {
            'date': datetime.strptime(committee_reading_date, '%b. %d, %Y'),
            'action_by': 'House of Assembly',
            'description': 'Committee Reading'
        }
        actions.append(action)

    if third_reading_date:
        action = {
            'date': datetime.strptime(third_reading_date, '%b. %d, %Y'),
            'action_by': 'House of Assembly',
            'description': 'Third Reading'
        }
        actions.append(action)

    if royal_assent_date:
        # Fix typo
        if royal_assent_date == 'Ar. 23, 2021':
            royal_assent_date = 'Apr. 23, 2021'

        action = {
            'date': datetime.strptime(royal_assent_date, '%b. %d, %Y'),
            'action_by': 'House of Assembly',
            'description': 'Royal Assent'
        }
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

def _remove_non_break_space(text):
    nonBreakSpace = u'\xa0'
    text = text.replace(nonBreakSpace, '')
    return text

def _get_bill_text_from_soup(soup):
    # Gets bill text from soup
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

    return bill_text

def main():
    print('\nSCRAPING NEWFOUNDLAND AND LABRADOR LEGISLATIONS\n')

    # Collect urls of session for latest assembly
    print(DEBUG_MODE and 'Collecting all sessions for latest NL assembly...\n' or '', end='')
    urls = get_urls()

    # Scrape urls
    print(DEBUG_MODE and 'Begin scraping each urls...\n' or '', end='')
    data = [scrape(url) for url in urls]

    # Write to database
    if not DEBUG_MODE:
        print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()