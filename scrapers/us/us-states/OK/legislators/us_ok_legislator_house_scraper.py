# Unavailable data - SourceID, seniority, military exp
# Wiki data - birthday, occupation, education 

import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from multiprocessing import Pool
from pprint import pprint
from nameparser import HumanName
import re
from tqdm import tqdm
import datetime

BASE_URL = 'https://okhouse.gov'
WIKI_URL = 'https://en.wikipedia.org'
SOUP_PARSER_TYPE = 'lxml'

STATE_ABBREVIATION = 'OK'
LEGISLATOR_TABLE_NAME = 'us_ok_legislators'

DEBUG_MODE = False
NUM_POOL_THREADS = 10
CURRENT_YEAR = datetime.datetime.now().year

scraper_utils = USStateLegislatorScraperUtils(STATE_ABBREVIATION, LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def get_urls():
    urls = []

    house_members_path = '/Members'
    house_members_list_path = '/Default.aspx'
    scrape_url = BASE_URL + house_members_path + house_members_list_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_RadGrid1_ctl00'})
    table_rows = table.find('tbody').find_all('tr')    
    for row in table_rows:
        path = row.find_all('td')[0].find('a').get('href')
        url = BASE_URL + house_members_path + '/' + path
        urls.append(url)

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    _set_most_recent_term_id(row, soup)
    _set_source_url(row, url)
    _set_name(row, soup)
    _set_party(row, soup)
    _set_role(row, soup)
    _set_years_active(row, soup)
    _set_phone_numbers(row, soup)
    _set_addresses(row, soup)
    _set_email(row, soup)
    _set_areas_served(row, soup)
    _set_district(row, soup)

    return row

def get_committee_urls():
    urls = []

    house_committees_path = '/Committees'
    house_committees_list_path = '/Default.aspx'

    # Get committees list
    scrape_url = BASE_URL + house_committees_path + house_committees_list_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    house_committees_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_dgrdCommittee_ctl00'}).find('tbody')
    house_committees_urls = [BASE_URL + house_committees_path + '/' + path.get('href')
        for path in house_committees_table.find_all('a')]

    conference_committees_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_rgdConference_ctl00'}).find('tbody')
    conference_committees_urls = [BASE_URL + house_committees_path + '/' + path.get('href')
        for path in conference_committees_table.find_all('a')]

    urls = house_committees_urls + conference_committees_urls

    return urls

def scrape_committee(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    committee_name = _get_committee_name(soup)
    members = _get_committee_list(soup, committee_name)

    return members

def update_house_committees(data, urls):
    print(DEBUG_MODE and 'Scraping committee data...\n' or '',  end='')
    # committees_data_list = [scrape_committee(url) for url in tqdm(urls[0:5])]
    with Pool(NUM_POOL_THREADS) as pool:
        committees_data_list = list(tqdm(pool.imap(scrape_committee, urls)))

    print(DEBUG_MODE and 'Updating committee data..\n' or '',  end='')
    for row in data:
        committees = _get_committees(committees_data_list, row.name_full, row.district)
        row.committees = committees

def get_wiki_urls_with_district():
    wiki_url_path = '/wiki/Oklahoma_House_of_Representatives'
    wiki_url = WIKI_URL + wiki_url_path

    soup = _create_soup(wiki_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    urls = []

    table_rows = soup.find('table', {'class', 'wikitable sortable'}).find('tbody').find_all('tr')

    for row in table_rows[1:]:
        district = row.find_all('td')[0].text.replace('\n', '')
        path = row.find_all('td')[1].find('a').get('href')

        if '/wiki' in path:
            url = {
                'district': district,
                'url': WIKI_URL + path
            }
            urls.append(url)

    return urls

def scrape_wiki(url):
    wiki_data = scraper_utils.scrape_wiki_bio(url['url'])
    wiki_crawl_delay = scraper_utils.get_crawl_delay(url['url'])
    scraper_utils.crawl_delay(wiki_crawl_delay)

    return {
        'data': wiki_data,
        'district': url['district']
    }

def merge_all_wiki_data(legislator_data, wiki_urls):
    print(DEBUG_MODE and 'Scraping wikipedia...\n' or '', end='')
    with Pool(NUM_POOL_THREADS) as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    print(DEBUG_MODE and 'Merging wikipedia data...\n' or '', end='')
    for data in wiki_data:
        _merge_wiki_data(legislator_data, data, years_active = False, most_recent_term_id = False)    

def _create_soup(url, soup_parser_type):
    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, soup_parser_type)
    return soup

def _set_most_recent_term_id(row, soup):
    year_elected_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblYear'}).text
    term_limited_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblTerm'}).text
    
    year_elected = int(year_elected_str.replace('Year: ', ''))
    term_limited = int(term_limited_str.replace('Year: ', ''))

    if year_elected <= CURRENT_YEAR and term_limited >= CURRENT_YEAR:
        row.most_recent_term_id = str(CURRENT_YEAR)
    else:
        row.most_recent_term_id = str(year_elected)

def _set_source_url(row, url):
    row.source_url = url

def _set_name(row, soup):
    name_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblName'}).text
    name_str = name_str.replace('\'', '')
    name = name_str.split(' ', 1)[1]
    human_name = HumanName(name)

    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, soup):
    party_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblParty'}).text
    row.party = party_str
    row.party_id = scraper_utils.get_party_id(party_str)

def _set_role(row, soup):
    role_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblName'}).text
    pattern = re.compile('[a-zA-Z]+')
    role = pattern.search(role_str).group(0)
    row.role = role

def _set_years_active(row, soup):
    year_elected_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblYear'}).text
    year_elected = int(year_elected_str.replace('Year: ', ''))
    row.years_active = [year for year in range(year_elected, CURRENT_YEAR + 1)]

def _set_phone_numbers(row, soup):
    phone_number_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblPhone'}).text
    phone_number_str = re.sub('[()]', '', phone_number_str)
    phone_number_str = re.sub(' ', '-', phone_number_str)

    phone_number = {
        'office': 'capital office',
        'number': phone_number_str
    }

    row.phone_numbers = [phone_number]

def _set_addresses(row, soup):
    address_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblCapitolRoom'}).find_parent('div').text

    address = {
        'location': 'capitol office',
        'address': _format_address_str(address_str),
    }

    row.addresses = [address]

def _set_email(row, soup):
    district_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblDistrict'}).text
    pattern = re.compile('[0-9]+')
    district = pattern.search(district_str).group(0)

    house_members_contact_path = '/Members/Contact.aspx'
    house_members_query = '?District='
    scrape_url = BASE_URL + house_members_contact_path + house_members_query + district
    
    email_soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    email = email_soup.find('input', {'id': 'txtMemberEmail'}).get('value')
    row.email = email
    

def _set_areas_served(row, soup):
    areas_served_county_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblCounties'}).text
    county_areas_served_list = areas_served_county_str.split(', ')

    areas_served_municipality_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblMunicipalities'}).text
    municipality_areas_served_list = areas_served_municipality_str.split(', ')

    row.areas_served = county_areas_served_list + municipality_areas_served_list

def _set_district(row, soup):
    district_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblDistrict'}).text
    pattern = re.compile('[0-9]+')
    district = pattern.search(district_str).group(0)
    row.district = district

def _format_address_str(original_str):
    address = original_str.strip()
    address = re.sub(' +', ' ', address)
    address = re.sub('\([0-9]+\) [0-9]{3}-[0-9]{4}|\r|\xa0', '', address)

    address = address.split('\n')
    new_address = [re.sub('\r|\xa0', '', a)
        for a in address]
    new_address = ','.join(new_address[:-1])

    new_address = re.sub(', Room, [0-9]{3}|\.', '', new_address)
    new_address = re.sub('OK ', 'OK, ', new_address)

    return new_address

def _get_committee_name(soup):
    # Format string version of HTML and convert back to soup
    committee_name_soup = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblHeader'})
    committee_name_soup = re.sub('<br/>', ' - ', str(committee_name_soup))
    committee_name_str = BeautifulSoup(committee_name_soup, 'lxml').text
    committee_name = committee_name_str.replace('\r\n', '')
    return committee_name

def _get_committee_list(soup, committee_name):
    members_list_soup = soup.find('div', {'id': 'ctl00_ContentPlaceHolder1_tblMembers'}).find_all('a')
    
    members = []
    members_href = []

    for member_soup in members_list_soup:
        if 'District=' in member_soup.get('href') and 'Photo' not in member_soup.get('href'):
            members_href.append(member_soup)

    pattern = re.compile('District=[0-9]+')

    for member_href in members_href:
        # Get district number
        link = str(member_href.get('href'))
        member_district = pattern.search(link).group(0)
        member_district = re.sub('District=', '', member_district)

        # Get name and position if available
        member_info = str(member_href).replace('<br/>', ',')
        member_info = BeautifulSoup(member_info, 'html.parser').text
        member_info = re.sub('Rep. |,\([A-Z]\) District [0-9]+', '', member_info).strip()
        member_info = [member.strip() for member in member_info.split(',')]

        # Leadership: [Chair, McEntire, Marcus]
        # Regular Member: [Roe, Cynthia]
        is_regular_member = len(member_info) < 3

        # Update values according to soup
        member = {}
        member['district'] = member_district
        member['role'] = 'member' if is_regular_member == True else member_info[0].lower()
        member['name'] = member_info[1] + ' ' + member_info[0] if is_regular_member == True else member_info[2] + ' ' + member_info[1]
        member['committee'] = committee_name

        members.append(member)

    return members

def _get_committees(committees_data_list, full_name, district):
    committees = []

    for committee_members in committees_data_list:
        for member in committee_members:
            if member['name'] == full_name and  member['district'] == district:
                committee = {
                    'role': member['role'],
                    'committee': member['committee'],
                }
                committees.append(committee)

    return committees

def _get_legislator_row(data, name_full, district):
    for row in data:
        if name_full == row.name_full and district == row.district:
            return row
    
    return None

def _merge_wiki_data(legislator_data, wiki_data, birthday=True, education=True, occupation=True, years_active=True, most_recent_term_id=True):
    full_name = wiki_data['data']['name_first'] + ' ' + wiki_data['data']['name_last']
    district = wiki_data['district']

    legislator_row = _get_legislator_row(legislator_data, full_name, district)

    if legislator_row == None:
        return

    for bio_info in wiki_data:
        if birthday == True:
            legislator_row.birthday = wiki_data['data']['birthday']
        if education == True:
            legislator_row.education = wiki_data['data']['education']
        if occupation == True:
            legislator_row.occupation = wiki_data['data']['occupation']
        if years_active == True:
            legislator_row.years_active = wiki_data['data']['years_active']
        if most_recent_term_id == True:
            legislator_row.most_recent_term_id = wiki_data['data']['most_recent_term_id']

def _fix_oddities(legislator_data):
    # Manually fixes odd data

    # District 41 - Change name - Crosswhite Hader together forms the last name
    legislator_row = _get_legislator_row(legislator_data, 'Denise Crosswhite Hader', '41')
    legislator_row.name_middle = ''
    legislator_row.name_last = 'CrosswhiteHader'

def scrape_house_legislators():
    # Collect house legislators urls
    print(DEBUG_MODE and 'Collecting house legislator URLs...\n' or '', end='')
    urls = get_urls()

    # Scrape data from collected URLs
    print(DEBUG_MODE and 'Scraping data from collected URLs...\n' or '', end='')
    with Pool(NUM_POOL_THREADS) as pool:
        data = list(tqdm(pool.imap(scrape, urls)))

    # Collect committee urls
    print(DEBUG_MODE and 'Collecting committee URLs...\n' or '', end='')
    committee_urls = get_committee_urls()

    # Update committee data
    print(DEBUG_MODE and 'Updating house legislators committees...\n' or '', end='')
    update_house_committees(data, committee_urls)

    # Collect wiki urls
    print(DEBUG_MODE and 'Collecting wiki URLs...\n' or '', end='')
    wiki_urls = get_wiki_urls_with_district()

    # Merge data from wikipedia
    print(DEBUG_MODE and 'Merging wiki data with house legislators...\n' or '', end='')
    merge_all_wiki_data(data, wiki_urls)

    # Manually fix any oddities
    print(DEBUG_MODE and 'Manually fixing oddities...\n' or '', end='')
    _fix_oddities(data)

    # Write to database
    print(DEBUG_MODE and 'Writing to database...\n' or '', end='')
    if DEBUG_MODE == False:
        scraper_utils.write_data(data)

# if __name__ == '__main__':
#     main()