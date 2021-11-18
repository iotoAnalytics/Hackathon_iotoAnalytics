# Unavailable data - source_url, source_id, addresses, seniority, military_experience
# Wiki data - birthday, education, occupation, years_active

import datetime
import multiprocessing
import os
import re
import sys
from multiprocessing import Pool

from bs4 import BeautifulSoup
from nameparser import HumanName
from pathlib import Path
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
from tqdm import tqdm
from unidecode import unidecode

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils

DEBUG_MODE = False

PROV_ABBREVIATION = 'NL'
LEGISLATOR_TABLE_NAME = 'ca_nl_legislators'
CURRENT_GENERAL_ASSEMBLY = '50'

BASE_URL = 'https://www.assembly.nl.ca'
MEMBER_PATH = '/members'
WIKI_URL = 'https://en.wikipedia.org'
SOUP_PARSER_TYPE = 'lxml'

NUM_POOL_PROCESSES = int(multiprocessing.cpu_count() * 0.5)
GENERAL_ASSEMBLY_YEAR = {
    '50': '2021'
}

scraper_utils = CAProvTerrLegislatorScraperUtils(PROV_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def scrape(url):
    options = Options()
    options.add_argument("--headless")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('disable-infobars')
    options.add_argument("--disable-extensions")
    options.add_argument('--User-Agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca')
    
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.switch_to.default_content()
    driver.get(BASE_URL + MEMBER_PATH + '/members.aspx')
    driver.maximize_window()
    sleep(25)

    html = driver.page_source
    print(html)
    soup = BeautifulSoup(html, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    data = []
    
    table_rows = soup.find('table', {'id': 'table'}).find('tbody').find_all('tr')

    for table_row in table_rows:
        row = scraper_utils.initialize_row()

        # Name, District(Areas Served), Party, Phone, Email
        fields = table_row.find_all('td')
        
        _set_most_recent_term_id(row, soup)
        _set_source_url(row, fields[4].text)
        _set_name(row, fields[0].text)
        _set_riding(row, fields[1].text)
        _set_party(row, fields[2].text)
        _set_phone_numbers(row, fields[3].text)
        _set_email(row, fields[4].text)
        _set_gender(row, fields[0])
        _set_wiki_url(row)

        data.append(row)

    driver.quit()

    return data

def get_roles():
    members_list_path = '/OfficeHolders.aspx'
    url = BASE_URL + MEMBER_PATH + members_list_path
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    lists = soup.find_all('ul', {'style': 'list-style:none;'})
    
    # Legislative Branch
    list_items = lists[0].find_all('li')
    roles_data = [_format_roles(li.text) for li in list_items]
    
    # Executive Branch
    formatted_roles = _format_roles(lists[1].find('li').text)
    roles_data.append(formatted_roles)
    
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
        if legislator:
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
    with Pool(NUM_POOL_PROCESSES) as pool:
        committees_data_list = list(tqdm(pool.imap(scrape_committee, urls)))

    for row in data:
        committees = _get_committees(committees_data_list, row.name_full, row.riding)
        row.committees = committees

def get_wiki_urls():
    wiki_url_path = '/wiki/Newfoundland_and_Labrador_House_of_Assembly'
    wiki_url = WIKI_URL + wiki_url_path

    soup = _create_soup(wiki_url, 'html.parser')

    infobox = soup.find('table', {'class':'infobox vcard'})
    current_assembly_url = WIKI_URL + infobox.find_all('tr')[1].td.a['href']

    soup = _create_soup(current_assembly_url, 'html.parser')
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
    with Pool(NUM_POOL_PROCESSES) as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    for data in wiki_data:
        _merge_wiki_data(legislator_data, data, most_recent_term_id=False)

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_most_recent_term_id(row, soup):
    content = soup.find_all('div', {'class': 'container'})[3]
    general_assembly_str = content.find('strong').text
    
    if CURRENT_GENERAL_ASSEMBLY in general_assembly_str:
        most_recent_term_id = GENERAL_ASSEMBLY_YEAR.get(CURRENT_GENERAL_ASSEMBLY)
        row.most_recent_term_id = most_recent_term_id

def _set_source_url(row, text):
    # Note: Members do not have a unique source url so email is used instead
    row.source_url = text

def _set_name(row, text):
    human_name = HumanName(text.replace('’', '\''))

    row.name_first = human_name.first
    row.name_last = human_name.last
    if row.name_last == 'Conway Ottenheimer':
        row.name_last = 'Conway-Ottenheimer'
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
    phone_numbers = []
    
    # Match format (###) ###-####
    match = re.search('\(([0-9]{3})\)\s([0-9]{3})\-([0-9]{4})', text)
    
    if match:
        # Format to ###-###-####
        number = match.group(1) + '-' + match.group(2) + '-' + match.group(3)
        phone_number = {
            'office': '',
            'number': number
        }
        phone_numbers.append(phone_number)

    row.phone_numbers = phone_numbers

def _set_email(row, text):
    row.email = text
    
def _set_riding(row, text):
    row.riding = text.replace(' - ', '-')

def _format_roles(text):
    # Format given text into role and name
    # e.g. Loyola O’Driscoll – Ferryland
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
        
        # Currently, there are no special roles listed for committees
        committee_member = {
            'name': member_data[0].strip(),
            'riding': member_data[1].strip().replace(' - ', '-'),
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

def _set_gender(row, td_element):
    try:
        url = td_element.a["href"]
        url = BASE_URL + url
        page_soup = _create_soup(url, 'html.parser')
        bio = page_soup.text
    except:
        bio = None
    row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio)

def _set_wiki_url(row):
    wiki_url_path = '/wiki/Newfoundland_and_Labrador_House_of_Assembly'
    wiki_url = WIKI_URL + wiki_url_path

    soup = _create_soup(wiki_url, 'html.parser')

    infobox = soup.find('table', {'class':'infobox vcard'})
    current_assembly_url = WIKI_URL + infobox.find_all('tr')[1].td.a['href']
    page_soup = _create_soup(current_assembly_url, 'html.parser')

    table = page_soup.find("table", {"class": "wikitable sortable"})
    trs = table.findAll("tr")[1:]
    for tr in trs:
        name_td = tr.findAll("td")[1]
        name = name_td.text
        district = tr.findAll("td")[3].text
        
        if "Vacant" in name:
            continue

        if unidecode(row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(row.name_last.lower()) in unidecode(name.strip().lower()):
            row.wiki_url = WIKI_URL + name_td.a["href"]    

def _get_legislator_row(legislator_data, wiki_url):
    for row in legislator_data:
        if wiki_url == row.wiki_url:
            return row

    return None

def _merge_wiki_data(legislator_data, wiki_data, wiki_url=True, birthday=True, education=True, occupation=True, years_active=True, most_recent_term_id=True):
    legislator_row = _get_legislator_row(legislator_data, wiki_data['wiki_url'])

    if not legislator_row:
        return

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
    if not DEBUG_MODE:
        print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
        scraper_utils.write_data(data)

    print('\nCOMPLETE!\n')

if __name__ == '__main__':
    main()
