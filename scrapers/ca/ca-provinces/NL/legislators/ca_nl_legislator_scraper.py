# Unavailable data - source_url, source_id, addresses, seniority, military_exp
# Wiki data - birthday, education, occupation, years_active, most_recent_term_id 

import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils
from bs4 import BeautifulSoup
from multiprocessing import Pool
from pprint import pprint
from nameparser import HumanName
import re
from tqdm import tqdm
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep

BASE_URL = 'https://www.assembly.nl.ca'
MEMBER_PATH = '/members'
WIKI_URL = 'https://en.wikipedia.org'
SOUP_PARSER_TYPE = 'lxml'

PROV_ABBREVIATION = 'NL'
LEGISLATOR_TABLE_NAME = 'ca_nl_legislators_test'

DEBUG_MODE = False
NUM_POOL_THREADS = 10
CURRENT_YEAR = datetime.datetime.now().year

PATH = '../../../../../web_drivers/chrome_win_90.0.4430.24/chromedriver.exe'

header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
options = Options()
options.headless = False

scraper_utils = CAProvTerrLegislatorScraperUtils(PROV_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def scrape(url):
    data = []

    driver = webdriver.Chrome(PATH, options=options)
    driver.switch_to.default_content()
    driver.get(BASE_URL + MEMBER_PATH + '/members.aspx')
    driver.maximize_window()
    sleep(2)

    html = driver.page_source
    soup = BeautifulSoup(html, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table_rows = soup.find('table', {'id': 'table'}).find('tbody').find_all('tr')

    for table_row in table_rows:
        row = scraper_utils.initialize_row()

        # Name, District(Areas Served), Party, Phone, Email
        fields = table_row.find_all('td')
        
        _set_source_url(row, fields[4].text)
        _set_name(row, fields[0].text)
        _set_riding(row, fields[1].text)
        _set_party(row, fields[2].text)
        _set_phone_numbers(row, fields[3].text)
        _set_email(row, fields[4].text)

        data.append(row)

    # roles_data = _get_roles()
    # _set_all_legislator_roles(data, roles_data)

    driver.quit()
    return data

def get_roles():
    url = BASE_URL + MEMBER_PATH + '/OfficeHolders.aspx'
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    lists = soup.find_all('ul', {'style': 'list-style:none;'})
    
    # Legislative Branch
    list_items = lists[0].find_all('li')
    roles_data = [_format_roles(li.text) for li in list_items]
    
    # Executive Branch
    roles_data.append(_format_roles(lists[1].find('li').text))
    
    return roles_data

def get_committee_urls():
    committee_url_path = '/Committees/StandingCommittees/'
    committee_url = BASE_URL + committee_url_path

    soup = _create_soup(committee_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    
    links = soup.find('ul', {'class': 'list-unstyled'}).find_all('a')
    urls = [BASE_URL + committee_url_path + link.get('href') for link in links]

    return urls

def update_legislator_roles(legislator_data, roles_data):
    # Default all role to MHA
    for legislator in legislator_data:
        legislator.role = 'Member of the House Assembly'

    # Set specific roles
    for role_data in roles_data:
        legislator = _get_legislator_row(legislator_data, role_data['name'])
        if legislator != None:
            legislator.role = role_data['role']

def scrape_committee(url):
    # Go to list of committee sessions
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    content = soup.find_all('div', {'class': 'container'})[3]
    committee_name = content.find('h1').text
    
    committee_members_url = url + content.find('ul').find('a').get('href')

    # Go to committee members for most recent session
    soup = _create_soup(committee_members_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    members = _get_committee_list(soup, committee_name)

    return members

def update_house_committees(data, urls):
    with Pool(NUM_POOL_THREADS) as pool:
        committees_data_list = list(tqdm(pool.imap(scrape_committee, urls)))

    for row in data:
        committees = _get_committees(committees_data_list, row.name_full, row.riding)
        row.committees = committees

def get_wiki_urls():
    wiki_url_path = '/wiki/Newfoundland_and_Labrador_House_of_Assembly'
    wiki_url = WIKI_URL + wiki_url_path

    soup = _create_soup(wiki_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    urls = []

    table_rows = soup.find('table', {'class', 'wikitable sortable'}).find('tbody').find_all('tr')

    for row in table_rows[1:]:
        name_full = row.find_all('td')[1].find('a').text
        path = row.find_all('td')[1].find('a').get('href')

        if '/wiki' in path:
            url = WIKI_URL + path
            urls.append(url)

    return urls

def scrape_wiki(url):
    wiki_data = scraper_utils.scrape_wiki_bio(url)
    wiki_crawl_delay = scraper_utils.get_crawl_delay(WIKI_URL)
    scraper_utils.crawl_delay(wiki_crawl_delay)

    return wiki_data

def merge_all_wiki_data(legislator_data, wiki_urls):
    with Pool(NUM_POOL_THREADS) as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    for data in wiki_data:
        _merge_wiki_data(legislator_data, data)

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_source_url(row, text):
    # Note: Members do not have a unique source url so email is used instead
    row.source_url = text

def _set_name(row, text):
    human_name = HumanName(text)

    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, text):
    if 'Independent' in text:
        text = 'Independent'
    elif 'New Democrat' in text:
        text = 'New Democratic'
    
    row.party = text
    row.party_id = scraper_utils.get_party_id(text)

def _set_phone_numbers(row, text):
    match = re.search('\(([0-9]{3})\)\s([0-9]{3})\-([0-9]{4})', text)
    if match:
        number = match.group(1) + '-' + match.group(2) + '-' + match.group(3)
        phone_number = {
            'office': '',
            'number': number
        }
        row.phone_numbers = [phone_number]

def _set_email(row, text):
    row.email = text
    
def _set_riding(row, text):
    row.riding = text

def _format_roles(text):
    text = re.split('-|–', text)
    role_data = {
        'role': text[0].replace('The ', '').strip(),
        'name': text[1].strip(),
    }
    return role_data

def _get_committee_list(soup, committee_name):
    content = soup.find_all('div', {'class': 'container'})[3]
    members = content.find('ul').find_all('li')
    
    committee_members = []
    for member in members:
        member_data = member.text.replace('–', '-').split('-', 1)
        
        # Currently, there are no roles listed
        committee_member = {
            'name': member_data[0].strip(),
            'riding': member_data[1].strip(),
            'role': 'member',
            'committee': committee_name,
        }
        committee_members.append(committee_member)

    return committee_members

def _get_committees(committees_data_list, full_name, riding):
    committees = []

    for committee_members in committees_data_list:
        for member in committee_members:
            if member['name'] == full_name and  member['riding'] == riding:
                committee = {
                    'role': member['role'],
                    'committee': member['committee'],
                }
                committees.append(committee)

    return committees

def _get_legislator_row(legislator_data, name_full):
    for row in legislator_data:
        if name_full == row.name_full:
            return row

    return None

def _merge_wiki_data(legislator_data, wiki_data, birthday=True, education=True, occupation=True, years_active=True, most_recent_term_id=True):
    full_name = wiki_data['name_first'] + ' ' + wiki_data['name_last']

    legislator_row = _get_legislator_row(legislator_data, full_name)

    if legislator_row == None:
        return

    for bio_info in wiki_data:
        if birthday == True:
            legislator_row.birthday = wiki_data['birthday']
        if education == True:
            legislator_row.education = wiki_data['education']
        if occupation == True:
            legislator_row.occupation = wiki_data['occupation']
        if years_active == True:
            legislator_row.years_active = wiki_data['years_active']
        if most_recent_term_id == True:
            legislator_row.most_recent_term_id = wiki_data['most_recent_term_id']

def main():
    print('NEWFOUNDLAND AND LABRADOR!')
    print('She\'s a rocky isle in the ocean ♫ ♫ ♫')
    print('And she\'s pounded by wind from the sea ♫ ♫ ♫')
    print('You might think that she\'s rugged and cold ♫ ♫ ♫')
    print('But she\'s home sweet home to me. ♫ ♫ ♫')

    print('\nSCRAPING NEWFOUNDLAND AND LABRADOR LEGISLATORS\n')

    # Scrape assembly members
    print(DEBUG_MODE and 'Scraping assembly members...\n' or '', end='')
    url = BASE_URL + MEMBER_PATH
    data = scrape(url)

    print(DEBUG_MODE and 'Collecting assembly members roles...\n' or '', end='')
    # Collect all roles of assembly
    roles_data = get_roles()

    # Update roles of assembly
    print(DEBUG_MODE and 'Updating assembly members roles...\n' or '', end='')
    update_legislator_roles(data, roles_data)

    # Collect committee urls
    print(DEBUG_MODE and 'Collecting committee URLs...\n' or '', end='')
    committee_urls = get_committee_urls()

    # Update committee data
    print(DEBUG_MODE and 'Updating house legislators committees...\n' or '', end='')
    update_house_committees(data, committee_urls)

    # Collect wiki urls
    print(DEBUG_MODE and 'Collecting wiki URLs...\n' or '', end='')
    wiki_urls = get_wiki_urls()

    # Merge data from wikipedia
    print(DEBUG_MODE and 'Merging wiki data with house legislators...\n' or '', end='')
    merge_all_wiki_data(data, wiki_urls)

    # Write to database
    if DEBUG_MODE == False:
        print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()