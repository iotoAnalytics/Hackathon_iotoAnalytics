# Unavailable data - email, seniority, military exp
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

BASE_URL = 'https://oksenate.gov'
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

    senate_members_path = '/senators'
    scrape_url = BASE_URL + senate_members_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    urls = [BASE_URL + path.get('href')
        for path in soup.find_all('a', {'class', 'sSen__sLink'})]

    return urls

def scrape(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)
    row = scraper_utils.initialize_row()

    bio_info = _retrieve_biography_info(soup)

    _set_source_id(row, soup)
    _set_most_recent_term_id(row, bio_info)
    _set_source_url(row, url)
    _set_name(row, soup)
    _set_party(row, bio_info)
    _set_role(row, soup)
    _set_years_active(row, bio_info)
    _set_phone_numbers(row, soup)
    _set_addresses(row, soup)
    _set_areas_served(row, soup)
    _set_district(row, soup)

    return row

def get_committee_urls():
    urls = []

    senate_committees_path = '/committees-list'

    # Get committees list
    scrape_url = BASE_URL + senate_committees_path
    soup = _create_soup(scrape_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    committee_urls = [BASE_URL + path.get('href')
        for path in soup.find_all('a', {'class', 'bTiles__item'})]
    
    # Get subcommittes list
    subcommittee_urls = []
    for url in tqdm(committee_urls):
        subcommittee_urls_list = _get_subcommittee_urls(url)
        subcommittee_urls = subcommittee_urls + subcommittee_urls_list

    # with Pool(NUM_POOL_THREADS) as pool:
        # subcommittee_urls = list(tqdm(pool.imap(_get_subcommittee_urls, committee_urls)))

    urls = committee_urls + subcommittee_urls

    return urls

def scrape_committee(url):
    soup = _create_soup(url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    committee_members = []

    committee_name = _get_committee_name(url, soup)
    leadership_members = _get_committee_leadership_list(soup, committee_name)
    regular_members = _get_committee_member_list(soup, committee_name)

    committee_members = leadership_members + regular_members

    return committee_members

def update_senate_committees(data, urls):
    committees_data_list = [scrape_committee(url) for url in tqdm(urls)]
    # with Pool(NUM_POOL_THREADS) as pool:
    #     committees_data_list = list(tqdm(pool.imap(scrape_committee, urls)))

    for row in data:
        committees = _get_committees(committees_data_list, row.name_full, row.source_id, row.district)
        row.committees = committees

def get_wiki_urls_with_district():
    wiki_url_path = '/wiki/Oklahoma_Senate'
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

def _set_source_id(row, soup):
    sid_str = soup.find('div', {'class', 'bSenBio__mail'}).find('a').get('href')
    sid_str = re.compile('sid=[0-9]+').search(sid_str).group(0)
    sid = re.compile('[0-9]+').search(sid_str).group(0)
    row.source_id = sid

def _set_most_recent_term_id(row, bio_info):
    # Senate website only showcase current senators
    row.most_recent_term_id = CURRENT_YEAR

def _set_source_url(row, url):
    row.source_url = url

def _set_name(row, soup):
    name_str = soup.find('div', {'class', 'bSenBio__title'}).text
    name = name_str.split(' ', 1)[1]
    human_name = HumanName(name)

    row.name_first = human_name.first
    row.name_last = human_name.last
    row.name_middle = human_name.middle
    row.name_suffix = human_name.suffix
    row.name_full = human_name.full_name

def _set_party(row, bio_info):
    party = bio_info['Party']

    if party == 'Democratic':
        party = 'Democrat'

    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

def _set_role(row, soup):
    role_str = soup.find('div', {'class', 'bSenBio__title'}).text
    role = role_str.split(' ')[0].replace('\n', '')
    row.role = role

def _set_years_active(row, bio_info):
    if 'Legislation Experience' in bio_info:
        years_active_list = bio_info['Legislation Experience']
        row.years_active = _format_years_active_str_list(years_active_list)

def _set_phone_numbers(row, soup):
    phone_number_str = soup.find('div', {'class': 'bSenBio__tel'}).find('a').text
    phone_number = re.sub('[()]', '', phone_number_str)
    phone_number = re.sub(' ', '-', phone_number)

    phone_number = {
        'office': 'district office',
        'number': phone_number,
    }

    row.phone_numbers = [phone_number]

def _set_addresses(row, soup):
    address_str = soup.find('div', {'class': 'bSenBio__address'}).find('p').text
    address = re.sub(' Rm. [0-9]+', '', address_str)
    address = re.sub('[.]+', '', address)
    address = re.sub('OK ', 'OK, ', address)
    
    address = {
        'location': 'district office',
        'adddress': address,
    }

    row.addresses = [address]

def _set_areas_served(row, soup):
    areas_served_content = soup.find('div', {'class', 'bDistrict'}).find_all('div', {'class', 'bDistrict__tr'})[1]
    areas_served_list = areas_served_content.find_all('div', {'class', 'bDistrict__td'})[0].find_all('li')
    areas_served = []

    for area in areas_served_list:
        county = area.text
        areas_served.append(county)

    row.areas_served = areas_served

def _set_district(row, soup):
    district_str = soup.find('div', {'class', 'bDistrict'}).find('h2').text
    district = district_str.strip().split()[1]
    row.district = district

def _retrieve_biography_info(soup):
    bio_info = [info.text.split(':', 1)
        for info in soup.find_all('div', {'class': 'bSenBio__infoIt'})]
    
    # Destructure bio
    bio_info = {info[0].replace('\n', ''): info[1].strip()
        for info in bio_info}

    return bio_info

def _normalize_years_active_string(years_active):
    normalized_years_active = re.sub(' ', '', years_active)
    normalized_years_active = re.sub('([Pp]resent|[Cc]urrent)', str(CURRENT_YEAR), normalized_years_active)
    return normalized_years_active

def _unpack_years_range(years_range):
    formatted_years_active = []
    formatted_years_range = [years_boundary.split('-') for years_boundary in years_range]
            
    for years_boundary in formatted_years_range:
        for year in range(int(years_boundary[0]), int(years_boundary[1]) + 1):
            if year not in formatted_years_active:
                formatted_years_active.append(year)

    return formatted_years_active

def _format_years_active_str_list(original_str):
    # ['2010-2014', '2014 - Present']
    years_active = re.compile('([0-9]+[ ]*-[0-9]+[ ]*|[0-9]+[ ]*-[ ]*[Pp]resent|[0-9]+[ ]*-[ ]*[Cc]urrent])').findall(original_str)

    # Remove spacings and change 'present' to numeric form
    years_active = list(map(lambda ya: _normalize_years_active_string(ya), years_active))

    # Unpack year range (e.g. 2019 - 2021 -> [2019, 2020, 2021])
    years_active = _unpack_years_range(years_active)

    return years_active

def _get_subcommittee_urls(committee_url):
    urls = []

    soup = _create_soup(committee_url, SOUP_PARSER_TYPE)
    scraper_utils.crawl_delay(crawl_delay)

    subcommittee_options = soup.find_all('a', {'class', 'bDrop__item select2-results__option'})

    if subcommittee_options != None:
        urls = [BASE_URL + path.get('href')
            for path in subcommittee_options]

    return urls

def _get_committee_name(url, soup):
    # e.g committee = /committees/education
    # e.g subcommittee = /committees/appropriations/education-sub
    committee_path = url.replace(BASE_URL, '').split('/')
    is_subcommittee = len(committee_path) < 3

    if is_subcommittee:
        parent_committee = committee_path[0]
        committee_name = parent_committee + '-' + soup.find('div', {'class', 'bTitle'}).find('h1').text.strip().lower()
    else:
        committee_name = soup.find('div', {'class', 'bTitle'}).find('h1').text.strip().lower()

    return committee_name

def _get_committee_leadership_list(soup, committee_name):
    leadership_members = soup.find_all('span', {'class', 'senators__item'})
    committee_members = _format_committee_leadership_members_list(leadership_members, committee=committee_name)
    return leadership_members

def _get_committee_member_list(soup, committee_name):
    regular_members = soup.find_all('div', {'class', 'senators__item'})
    committee_members = _format_committee_regular_members_list(regular_members, committee=committee_name)
    return committee_members

def _get_committees(committees_data_list, full_name, source_id, district):
    committees = []

    for committee_members in committees_data_list:
        for member in committee_members:
            if ('source_id' in member and member['source_id'] == source_id or
                'district' in member and member['district'] == district) and \
                member['name'] == full_name:

                committee = {
                    'role': member['role'],
                    'committee': member['committee'],
                }

                committees.append(committee)

    return committees

def _format_committee_leadership_members_list(leadership_members, sid='', name='', position='', committee=''):
    members = []

    for member in leadership_members:
        if member.find('article'):
            sid = member.find('article').get('data-history-node-id')
            name = member.find('span', {'class', 'senators__name'}).text.replace('\n', '')
            position = member.find('span', {'class', 'senators__position'}).text.replace('\n', '').strip().lower()
            
            committee_member = {
                "source_id": sid,
                "name": name,
                "role": position,
                "committee": committee,
            }

            members.append(committee_member)
    
    return members

def _format_committee_regular_members_list(regular_members, district='', name='', position='', committee=''):
    members = []

    for member in regular_members:
        district = member.find('span', {'class', 'sSen__sDis'}).text.replace('District ', '')
        name = member.find('span', {'class', 'sSen__sName'}).text.strip()
        
        committee_member = {
            "district": district,
            "name": name,
            "role": "member",
            "committee": committee,
        }

        members.append(committee_member)
    
    return members

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

    # District 35 - Change name - Jo Anna together forms the first name
    legislator_row = _get_legislator_row(legislator_data, 'Jo Anna Dossett', '35')
    legislator_row.name_middle = ''
    legislator_row.name_first = 'JoAnna'

def scrape_senate_legislators():
    # Collect senate legislators urls
    print(DEBUG_MODE and 'Collecting senate legislator URLs...\n' or '', end='')
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
    update_senate_committees(data, committee_urls)

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