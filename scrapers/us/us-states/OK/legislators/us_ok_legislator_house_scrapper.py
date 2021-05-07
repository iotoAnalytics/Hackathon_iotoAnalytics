# TODO - Modify merge wiki data to merge on district instead of name
# TODO - Refactor
# TODO - Use pool to speed up process
# TODO - Try/Exception

# Unavailable data - SourceID, email, seniority, military exp
# Wiki data - birthday, occupation, education 

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
from tqdm import tqdm
import datetime

now = datetime.datetime.now()

state_abbreviation = 'OK'
database_table_name = 'us_ok_legislators_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://okhouse.gov'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def get_urls():
    urls = []

    house_members_path = '/Members'
    house_members_list_path = '/Default.aspx'
    scrape_url = base_url + house_members_path + house_members_list_path
    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    table_rows = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_RadGrid1_ctl00'}).find('tbody').find_all('tr')    
    for tr in table_rows:
        path = tr.find_all('td')[0].find('a').get('href')
        urls.append(base_url + house_members_path + '/' + path)

    return urls

def scrape(url):
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    row = scraper_utils.initialize_row()

    # Most Recent Term ID
    original_year_elected = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblYear'}).text
    original_term_limited = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblTerm'}).text
    year_elected = int(format_year_label_str(original_year_elected))
    term_limited = int(format_year_label_str(original_term_limited))

    if year_elected <= now.year and term_limited >= now.year:
        row.most_recent_term_id = str(now.year)
    else:
        row.most_recent_term_id = str(year_elected)

    # Source URL
    row.source_url = url

    # Name (Full, last, first, middle, suffix)
    original_name_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblName'}).text
    name_dict = format_name_str(original_name_str)
    row.name_full = name_dict['full']
    row.name_last = name_dict['last']
    row.name_first = name_dict['first']
    row.name_middle = name_dict['middle']
    row.name_suffix = name_dict['suffix']
    
    # Party ID + Party
    row.party = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblParty'}).text
    row.party_id = scraper_utils.get_party_id(row.party)

    # Role
    original_role_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblName'}).text
    row.role = format_role_str(original_role_str)

    # Years active
    row.years_active = [year for year in range(year_elected, now.year + 1)]

    # Committees (Peformed under different scraping)
    
    # Phone Numbers
    original_phone_number_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblPhone'}).text
    phone_number = {
        'office': 'capital office',
        'number': format_phone_number_str(original_phone_number_str)
    }
    row.phone_numbers = [phone_number]

    # Addressess
    original_address_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblCapitolRoom'}).find_parent('div').text
    address = {
        'location': 'capitol office',
        'address': format_address_str(original_address_str),
    }
    row.addresses = [address]

    # Areas Served
    original_area_served_county_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblCounties'}).text
    county_areas_served = format_areas_served(original_area_served_county_str)

    original_area_served_municipality_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblMunicipalities'}).text
    municipality_areas_served = format_areas_served(original_area_served_municipality_str)

    row.areas_served = county_areas_served + municipality_areas_served

    # District
    original_district_str = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblDistrict'}).text
    row.district = format_district_str(original_district_str)

    return row

def get_committee_urls():
    urls = []

    house_committees_path = '/Committees'
    house_committees_list_path = '/Default.aspx'

    # Get committees list
    scrape_url = base_url + house_committees_path + house_committees_list_path
    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    house_committees_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_dgrdCommittee_ctl00'}).find('tbody')
    conference_committees_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_rgdConference_ctl00'}).find('tbody')

    house_committees_urls = [base_url + house_committees_path + '/' + path.get('href')
        for path in house_committees_table.find_all('a')]
    conference_committees_urls = [base_url + house_committees_path + '/' + path.get('href')
        for path in conference_committees_table.find_all('a')]

    urls = house_committees_urls + conference_committees_urls

    return urls

def scrape_committee(url):
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')

    committee_members = []

    # Get committee name
    committee_name = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_lblHeader'})
    committee_name = re.sub('<br/>', ' - ', str(committee_name)) # format for subcommittee
    committee_name = BeautifulSoup(committee_name, 'html.parser')
    committee_name = format_committee_name_str(committee_name.text)
    
    # Get members
    members_list_soup = soup.find('div', {'id': 'ctl00_ContentPlaceHolder1_tblMembers'}).find_all('a')
    members = format_committee_members_list_soup(members_list_soup, committee=committee_name)

    return members

def update_committee_data(data, urls):
    print('Scraping committee data...')
    # committees_data_list = [scrape_committee(url) for url in tqdm(urls[0:5])]
    with Pool() as pool:
        committees_data_list = list(tqdm(pool.imap(scrape_committee, urls)))
    committees = []

    print('Updating committee data..')
    for committee_members in committees_data_list:
        for member in committee_members:
            for legislator in data: # Match member in committee to legislator row and update committee field
                # pprint(member)
                if 'district' in member and member['district'] == legislator.district:
                    committee = {
                        'role': member['role'],
                        'committee': member['committee'],
                    }
                    # committees.append(committee)
                    legislator.committees.append(committee)
    

def format_year_label_str(original_str):
    year = original_str.replace('Year: ', '')
    return year

def format_name_str(original_str):
    name_dict = {
        'full': '',
        'first': '',
        'last': '',
        'middle': '',
        'suffix': '',
    }

    p = re.compile('[a-zA-Z]+')
    name = p.findall(original_str)
    name_dict['full'] = name[1] + ' ' + name[2]
    name_dict['first'] = name[1]
    name_dict['last'] = name[2]

    return name_dict

def format_phone_number_str(original_str):
    phone_number = re.sub('[()]', '', original_str)
    phone_number = re.sub(' ', '-', phone_number)
    return phone_number

def format_address_str(original_str):
    address = original_str.strip()
    address = re.sub(' +', ' ', address)
    address = re.sub('\([0-9]+\) [0-9]{3}-[0-9]{4}|\r|\xa0', '', address)

    # TODO - Refactor this weird bug fix
    address = address.split('\n')

    new_address = [re.sub('\r|\xa0', '', a)
        for a in address]
    
    new_address = ','.join(new_address[:-1])

    new_address = re.sub(', Room, [0-9]{3}|\.', '', new_address)
    new_address = re.sub('OK ', 'OK, ', new_address)

    return new_address

def format_role_str(original_str):
    p = re.compile('[a-zA-Z]+')
    role = p.search(original_str).group(0)
    return role

def format_areas_served(original_str):
    areas_served = original_str.split(', ')
    return areas_served

def format_district_str(original_str):
    p = re.compile('[0-9]+')
    district = p.search(original_str).group(0)
    return district

def format_committee_name_str(original_str):
    return original_str.replace('\r\n', '')

def format_committee_members_list_soup(members_list_soup, district='', name='', position='', committee=''):
    committee_members = []
    members_href = []

    for member_soup in members_list_soup:
        if 'District=' in member_soup.get('href') and 'Photo' not in member_soup.get('href'):
            # print(member_soup.get('href'))
            members_href.append(member_soup)

    p = re.compile('District=[0-9]+')

    for member_href in members_href:
        # Get district number
        link = str(member_href.get('href'))
        member_district = p.search(link).group(0)
        member_district = re.sub('District=', '', member_district)

        # Get name and position if available
        member_info = str(member_href).replace('<br/>', ',')
        member_info = BeautifulSoup(member_info, 'html.parser').text
        member_info = re.sub('Rep. |,\([A-Z]\) District [0-9]+', '', member_info).strip()
        member_info = [member.strip().lower() for member in member_info.split(',')]

        # Leadership: [Chair, McEntire, Marcus]
        # Regular Member: [Roe, Cynthia]
        is_regular_member = len(member_info) < 3

        # Set default values
        member = {
            'district': district,
            'role': position,
            'name': name,
            'committee': committee
        }

        # Update values according to soup
        member['district'] = member_district
        member['role'] = 'member' if is_regular_member == True else member_info[0]
        member['name'] = member_info[1] + ' ' + member_info[0] if is_regular_member == True else member_info[2] + ' ' + member_info[1]

        committee_members.append(member)
    
    return committee_members

def get_wiki_urls():
    wiki_base_url = 'https://en.wikipedia.org'
    wiki_url_path = '/wiki/Oklahoma_House_of_Representatives'

    page = scraper_utils.request(wiki_base_url + wiki_url_path)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    urls = []

    table_rows = soup.find('table', {'class', 'wikitable sortable'}).find('tbody').find_all('tr')
    # print(table_rows)
    for tr in table_rows[1:]:
        url_path = tr.find_all('td')[1].find('a').get('href')
        if '/wiki' in url_path:
            urls.append(wiki_base_url + url_path)

    return urls

def get_wiki_district_urls():
    wiki_base_url = 'https://en.wikipedia.org'
    wiki_url_path = '/wiki/Oklahoma_House_of_Representatives'

    page = scraper_utils.request(wiki_base_url + wiki_url_path)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'lxml')

    urls = []

    table_rows = soup.find('table', {'class', 'wikitable sortable'}).find('tbody').find_all('tr')
    # print(table_rows)
    for tr in table_rows[1:]:
        district = tr.find_all('td')[0]
        url_path = tr.find_all('td')[1].find('a').get('href')
        if '/wiki' in url_path:
            url = {
                'district': district,
                'url': wiki_base_url + url_path
            }
            urls.append(url)

    return urls

def _get_legislator_row(data, name_first, name_last):
    for row in data:
        if name_first == row.name_first and name_last == row.name_last:
            return row
    
    return None

def _merge_wiki_data(legislator_data, wiki_data, birthday=True, education=True, occupation=True, years_active=True, most_recent_term_id=True):
    legislator_row = _get_legislator_row(legislator_data, wiki_data['name_first'], wiki_data['name_last'])

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

def merge_all_wiki_data(legislator_data, wiki_urls):
    print('Scraping wikipedia...')
    with Pool() as pool:
        wiki_data = list(tqdm(pool.imap(scrape_wiki, wiki_urls)))

    # for url in tqdm(wiki_urls):
    #     wiki_data = scraper_utils.scrape_wiki_bio(url)
    #     wiki_crawl_delay = scraper_utils.get_crawl_delay(url)
    #     scraper_utils.crawl_delay(wiki_crawl_delay)

    print('Merging wikipedia data with legislator...')
    for data in tqdm(wiki_data):
        _merge_wiki_data(legislator_data, data, years_active = False, most_recent_term_id = False)    

def scrape_wiki(url):
    wiki_data = scraper_utils.scrape_wiki_bio(url)
    wiki_crawl_delay = scraper_utils.get_crawl_delay(url)
    scraper_utils.crawl_delay(wiki_crawl_delay)

    return wiki_data

if __name__ == '__main__':
    # Collect house legislators urls
    urls = get_urls()

    # Scrape data from collected URLs
    print('Scraping house legislators...')
    with Pool() as pool:
        data = list(tqdm(pool.imap(scrape, urls[0:5])))
    pprint(data[0:2])

    # Collect committee urls
    committee_urls = get_committee_urls()

    # Update committee data
    print('Updating house legislators committees...')
    update_committee_data(data, committee_urls)

    # # Collect wiki urls
    wiki_urls = get_wiki_urls()

    # # Fill in data from wikipedia
    print('Merging wiki data with house legislators ...')
    merge_all_wiki_data(data, wiki_urls)

    # Write to database
    print('Writing to database')
    scraper_utils.write_data(data)

    print('Complete!')