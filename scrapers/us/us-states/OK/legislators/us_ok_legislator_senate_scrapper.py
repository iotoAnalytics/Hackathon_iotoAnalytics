# TODO - Fix J.J Dossett Name
# TODO - Change Database table name for production
# TODO - Scrape from wiki - birthday, most_recent_term_id, education, occupation
    # firstname, lastname, birthday, education, occupation, years_active, most_recent_term_id can be found with scrape_wiki_bio
# TODO - Try/Exception for future logs 


import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
# import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
# from nameparser import HumanName
import re
# import boto3
# import time
# import pandas as pd
import datetime


now = datetime.datetime.now()

state_abbreviation = 'OK'
database_table_name = 'us_ok_legislators_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://oksenate.gov'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    senate_members_path = '/senators'
    scrape_url = base_url + senate_members_path
    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    urls = [base_url + path.get('href')
        for path in soup.find_all('a', {'class', 'sSen__sLink'})]

    return [urls[1]]

def scrape(url):
    # Send request to website
    # Delay so we don't overburden web servers
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    row = scraper_utils.initialize_row()

    # Source Id
    original_source_id_str = soup.find('div', {'class', 'bSenBio__mail'}).find('a').get('href')
    row.source_id = format_source_id_str(original_source_id_str)

    # TODO - Most Recent Term

    # Source URL
    row.source_url = url

    # Name (full, last, first, middle, suffix) + Role
    name_info_dict = retrieve_name_info(soup)

    if name_info_dict['suffix'] != None:
        row.name_suffix = name_info_dict['suffix']
    if name_info_dict['firstname'] != None:
        row.name_first = name_info_dict['firstname']
    if name_info_dict['middlename'] != None:
        row.name_middle = name_info_dict['middlename']
    if name_info_dict['lastname'] != None:
        row.name_last = name_info_dict['lastname']
    if name_info_dict['fullname'] != None:
        row.name_full = name_info_dict['fullname']
    if name_info_dict['role'] != None:
        row.role = name_info_dict['role']

    # TODO - Refactor since it's only being used for party
    # Get Bio Info - Usually contains party
    bio_info = retrieve_biography_info(soup)

    # Party ID + Party
    original_party_str = bio_info['Party']
    party = format_party_str(original_party_str)

    if party != None:
        row.party_id = scraper_utils.get_party_id(party)
        row.party = party

    # Years Active
    original_years_active_str_list = bio_info['Legislation Experience']
    row.years_active = format_years_active_str_list(original_years_active_str_list)

    # Phone Number
    original_phone_number_str = soup.find('div', {'class': 'bSenBio__tel'}).find('a').text
    phone_number = {
        'office': 'district office',
        'number': format_phone_number_str(original_phone_number_str),
    }
    row.phone_numbers.append(phone_number)

    # Address
    original_address_str = soup.find('div', {'class': 'bSenBio__address'}).find('p').text
    address = {
        'location': 'district office',
        'adddress': format_address_str(original_address_str),
    }
    row.addresses.append(address)

    # TODO - Email

    # TODO - Birthday

    # TODO - Seniority*

    # TODO - Occupation

    # TODO - Education

    # TODO - Military Exp*

    # Areas Served
    original_areas_served_str = soup.find('div', {'class', 'bDistrict'}).find_all('div', {'class', 'bDistrict__tr'})[1].find_all('div', {'class', 'bDistrict__td'})[0].find_all('li')
    for area in original_areas_served_str:
        row.areas_served.append(area.text)

    # District
    original_district_str = soup.find('div', {'class', 'bDistrict'}).find('h2').text
    row.district = format_district_str(original_district_str)

    return row

def _get_subcommittee_urls(committee_url):
    print(committee_url)

    urls = []

    page = scraper_utils.request(committee_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    subcommittee_options = soup.find_all('a', {'class', 'bDrop__item select2-results__option'})

    if subcommittee_options != None:
        urls = [base_url + path.get('href')
            for path in subcommittee_options]

    return urls

def get_committee_urls():
    urls = []

    senate_committees_path = '/committees-list'

    # Get committees list
    scrape_url = base_url + senate_committees_path
    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    committee_urls = [base_url + path.get('href')
        for path in soup.find_all('a', {'class', 'bTiles__item'})]
    urls = urls + committee_urls

    # TODO - Can use Multiprocessing if needed
    # Get subcommittes list
    for url in committee_urls:
        urls = urls + _get_subcommittee_urls(url)

    return urls

def scrape_committee(url):
    # Delay so we don't overburden web servers
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')

    committee_members = []

    # Get committee name
    # e.g committee = /committees/education
    # e.g subcommittee = /committees/appropriations/education-sub
    committee_path = url.replace(base_url, '').split('/')
    is_subcommittee = len(committee_path) < 3

    committee_name = ''
    if is_subcommittee:
        parent_committee = committee_path[0]
        committee_name = parent_committee + '-' + soup.find('div', {'class', 'bTitle'}).find('h1').text.strip().lower()
    else:
        committee_name = soup.find('div', {'class', 'bTitle'}).find('h1').text.strip().lower()

    # Get leadership members
    leadership_members = soup.find_all('span', {'class', 'senators__item'})
    committee_members = committee_members + format_committee_leadership_members_list(leadership_members, committee=committee_name)

    # Get regular members
    regular_members = soup.find_all('div', {'class', 'senators__item'})
    committee_members = committee_members + format_committee_regular_members_list(regular_members, committee=committee_name)

    # print(committee_members)
    return committee_members

def update_committee_data(data, urls):
    # print(urls)
    # TODO - Consider Multiprocessing
    committees_data_list = [scrape_committee(url) for url in urls]

    for committee_members in committees_data_list:
        for member in committee_members:
            for legislator in data: # Match member in committee to legislator row and update committee field
                if 'source_id' in member and member['source_id'] == legislator.source_id or \
                    'district' in member and member['district'] == legislator.district:
                    legislator.committees.append({'role': member['role'], 'committee': member['committee'],})

    return data

# Refactor name_info to get necessary info
def retrieve_name_info(soup):
    try:
        name_info_dict = {'suffix': None, 'firstname': None,
            'middlename': None, 'lastname': None, 'fullname': None, 'role': None}

        name_content = soup.find('span', {'class', 'field--name-title'}).text
        name_content = name_content.split(' ')

        for i in range(len(name_content)):
            if i == 0:
                name_info_dict['firstname'] = name_content[i]
            elif i == 1 and len(name_content) > 2:
                name_info_dict['middlename'] = name_content[i]
            else:
                name_info_dict['lastname'] = name_content[i]
            name_info_dict['role'] = 'Senator'

        fullname = ''
        if name_info_dict['suffix'] != None:
            fullname += name_info_dict['suffix'] + ' '
        if name_info_dict['firstname'] != None:
            fullname += name_info_dict['firstname'] + ' '
        if name_info_dict['middlename'] != None:
            fullname += name_info_dict['middlename'] + ' '
        if name_info_dict['lastname'] != None:
            fullname += name_info_dict['lastname'] + ' '

        fullname = fullname[:len(fullname) - 1]

        if fullname != '':
            name_info_dict['fullname'] = fullname

        return name_info_dict
    except:
        # raise
        sys.exit('error in retrieving name information\n')

def retrieve_biography_info(soup):
    bio_info = [info.text.split(':', 1)
        for info in soup.find_all('div', {'class': 'bSenBio__infoIt'})]
    
    # Destructure bio
    bio_info = {info[0].replace('\n', ''): info[1].strip()
        for info in bio_info}

    return bio_info

def format_source_id_str(original_str):
    sid_string = re.compile('sid=[0-9]+').search(original_str).group(0)
    sid = re.compile('[0-9]+').search(sid_string).group(0)
    return sid

def format_party_str(original_str):
    if original_str == 'Democratic':
        original_str = 'Democrat'
    return original_str

def _normalize_years_active_string(years_active):
    normalized_years_active = re.sub(' ', '', years_active)
    normalized_years_active = re.sub('[Pp]resent', str(now.year), normalized_years_active)
    return normalized_years_active

def _unpack_years_range(years_range):
    formatted_years_active = []
    formatted_years_range = [years_boundary.split('-') for years_boundary in years_range]
            
    for years_boundary in formatted_years_range:
        for year in range(int(years_boundary[0]), int(years_boundary[1]) + 1):
            if year not in formatted_years_active:
                formatted_years_active.append(year)

    return formatted_years_active

def format_years_active_str_list(original_str):
    # ['2010-2014', '2014 - Present']
    years_active = re.compile('([0-9]+[ ]*-[0-9]+[ ]*|[0-9]+[ ]*-[ ]*[Pp]resent)').findall(original_str)

    # Remove spacings and change 'present' to numeric form
    years_active = list(map(lambda ya: _normalize_years_active_string(ya), years_active))

    # Unpack year range (e.g. 2019 - 2021 -> [2019, 2020, 2021])
    years_active = _unpack_years_range(years_active)

    return years_active

def format_phone_number_str(original_str):
    phone_number = re.sub('[()]', '', original_str)
    phone_number = re.sub(' ', '-', phone_number)
    return phone_number

def format_address_str(original_str):
    address = re.sub(' Rm. [0-9]+', '', original_str)
    address = re.sub('[.]+', '', address)
    address = re.sub('OK ', 'OK, ', address)
    return address

def format_district_str(original_str):
    return original_str.strip().split()[1]

def format_committee_leadership_members_list(leadership_members, sid='', name='', position='', committee=''):
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

def format_committee_regular_members_list(regular_members, district='', name='', position='', committee=''):
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


# def get_wiki_urls():
#     wiki_base_url = 'https://en.wikipedia.org'
#     wiki_url_path = '/wiki/Oklahoma_Senate'

#     page = scraper_utils.request(wiki_base_url + wiki_url_path)
#     scraper_utils.crawl_delay(crawl_delay)
#     soup = BeautifulSoup(page.content, 'lxml')

#     urls = []

#     table_rows = soup.find('table', class_='wikitable sortable').find('tbody').find_all('tr')
#     # print(table_rows)
#     for tr in table_rows[1:]:
#         url_path = tr.find_all('td')[1].find('a').get('href')
#         if '/wiki' in url_path:
#             urls.append(wiki_base_url + url_path)

#     return urls

if __name__ == '__main__':
    urls = get_urls()
    print(urls)

    # Scrape data from collected URLs serially, which is slower:
    data = [scrape(url) for url in urls]
    print(data)
    # Speed things up using pool.
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    # Update committees data
    committee_urls = get_committee_urls()
    update_committee_data(data, committee_urls)

    print(data)

    # Once we collect the data, we'll write it to the database:
    # scraper_utils.write_data(data)

    # wiki_urls = get_wiki_urls()[1]
    # print(wiki_urls)

    # deets = scraper_utils.scrape_wiki_bio('https://en.wikipedia.org/wiki/Mark_Allen_(politician)')
    # pprint(deets)

    print('Complete!')
