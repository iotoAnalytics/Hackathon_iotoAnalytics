'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import sys
import os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from nameparser import HumanName
from database import Database
from pprint import pprint
import re
import boto3
import time

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

state_abbreviation = 'OH'
database_table_name = 'us_oh_legislators_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

house_url = 'https://www.legislature.ohio.gov/legislators/house-directory'
senate_url = 'https://www.legislature.ohio.gov/legislators/senate-directory'

base_url = 'https://www.legislature.ohio.gov'
senate_committee = 'https://ohiosenate.gov/committees'
base_sen = 'http://ohiosenate.gov/'

wiki_link = 'https://en.wikipedia.org/wiki/Ohio_General_Assembly'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_legislator_wiki_link(name_first, name_last, role):
    response = requests.get(wiki_link)
    soup = BeautifulSoup(response.content, 'lxml')
    if role == 'Senator':
        legislator_lst = soup.find('div', {'aria-labelledby': 'Current_members_of_the_Ohio_Senate'}).find_all('li')
    elif role == 'Representative':
        legislator_lst = soup.find('div', {'aria-labelledby': 'Members_of_the_Ohio_House_of_Representatives'}).find_all(
            'li')
    for legislator in legislator_lst:
        try:
            legislator_wiki_title = legislator.find('a').get('title')
            if name_first in legislator_wiki_title and name_last in legislator_wiki_title:
                return legislator.find('a').get('href')
        except TypeError:
            pass
    return None


def get_rep_coms(link):
    split = 'http://ohiohouse.gov/'
    name = link.split(split)[1]
    com_list_url = f'{split}members/{name}/committees'
    response = requests.get(com_list_url)
    soup = BeautifulSoup(response.content, 'lxml')
    com_link_html = soup.find('div', {'class': 'gray-block'}).find_all('a')
    com_links = [split[:-1] + x.get('href') for x in com_link_html]
    coms_lst = []
    for link in com_links:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, 'lxml')
        div_lst = soup.find('div', {'class': 'gray-block'}).find_all('div')[1:]
        for item in div_lst:
            try:
                compare_name = item.find('a').get('href')
                if name in compare_name:
                    try:
                        position = item.find('div', {'class': 'committee-member-position'}).text
                    except:
                        position = 'member'
                    coms_lst.append({
                        'committee': ' '.join(link.split('/')[-1].split('-')),
                        'role': position
                    })
            except AttributeError:
                pass
        scraper_utils.crawl_delay(crawl_delay)

    scraper_utils.crawl_delay(crawl_delay)
    return coms_lst


def get_sen_coms(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    com_lst = soup.find('div', {'class': 'committeeGrid'}).find_all('a')
    com_lst = [base_sen + x.get('href') for x in com_lst]
    com_dict = {}
    for link in com_lst:
        com = link.split('/')[-1]
        com_soup = BeautifulSoup(requests.get(link).content, 'lxml')
        scraper_utils.crawl_delay(crawl_delay)
        legis_lst = com_soup.find('div', {'class': 'portraitGroupModule'}).find_all('div')
        for item in legis_lst:
            try:
                name = item.find('div', {'class': 'profileName'}).text
                position = item.find('div', {'class': 'profilePosition'})
                link = item.find('div', {'class': 'profileName'}).find('a').get('href')
                link = base_sen + link.replace('../', '')
                current_com = {'role': 'member' if not position else position.text, 'committee': com}
                if link not in com_dict:
                    com_dict[link] = {'name': name,
                                      'committees': [current_com]}
                else:
                    com_dict[link]['committees'].append(current_com)
            except:
                pass
    return com_dict


def get_legislator_links(url):
    los = []

    request = requests.get(url)
    soup = BeautifulSoup(request.content, 'lxml')
    legislator_table = soup.find('div', {'class': 'mediaGrid mediaGridDirectory'}).find_all('a')
    role = 'Representative' if 'house-directory' in url else 'Senator'

    for legislator in legislator_table:
        link = legislator.get('href')
        name = legislator.find('div', {'class': 'mediaCaptionTitle'}).text
        hn = HumanName(name)
        info = legislator.find('div', {'class': 'mediaCaptionSubtitle'}).text.split('|')
        district = info[0].replace('District', '').strip()
        party = 'Republican' if info[1].strip() == 'R' else 'Democrat'
        los.append({
            'link': link,
            'name_full': name,
            'name_first': hn.first,
            'name_last': hn.last,
            'name_middle': hn.middle,
            'name_suffix': hn.suffix,
            'party': party,
            'role': role,
            'district': district
        })

    scraper_utils.crawl_delay(crawl_delay)

    return los


def scrape(legis_dict):
    # Send request to website
    url = legis_dict['link']
    row = scraper_utils.initialize_row()

    row.source_url = url
    row.name_full = legis_dict['name_full'].title()
    row.name_first = legis_dict['name_first'].title()
    row.name_last = legis_dict['name_last'].title()
    row.name_middle = legis_dict['name_middle']
    row.name_suffix = legis_dict['name_suffix']

    row.district = legis_dict['district']
    role = legis_dict['role']
    row.role = role

    party = legis_dict['party']
    row.party = party
    row.party_id = scraper_utils.get_party_id(party)

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    if role == 'Representative':
        page_info = list(filter(None, soup.find_all('div', {'class': 'member-info-bar-module'})[-1].text.split('\n')))
        row.addresses = [{
            'location': 'district office',
            'address': page_info[0]
        }]
        row.phone_numbers = [{
            'office': 'district office',
            'number': page_info[1]
        }]
    elif role == 'Senator':
        page_info = soup.find('div', {'class': 'generalInfoModule'})
        address = page_info.find('div', {'class': 'address'}).text.replace('\n', '').strip()
        row.addresses = [{
            'location': 'district office',
            'address': address
        }]

        phone = page_info.find('div', {'class': 'phone'}).text.replace('\n', '').strip()
        row.phone_numbers = [{
            'office': 'district office',
            'number': phone
        }]

    row.committees = get_rep_coms(url) if role == 'Representative' else legis_dict['committees']

    legis_wiki_link = get_legislator_wiki_link(legis_dict['name_first'], legis_dict['name_last'], role)
    if legis_wiki_link:
        wiki_info = scraper_utils.scrape_wiki_bio(legis_wiki_link)
        row.birthday = wiki_info['birthday']
        row.education = wiki_info['education']
        row.occupation = wiki_info['occupation']
        row.years_active = wiki_info['years_active']
        row.most_recent_term_id = wiki_info['most_recent_term_id']

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


if __name__ == '__main__':
    house_dicts = get_legislator_links(house_url)
    sen_dicts = get_legislator_links(senate_url)
    sen_coms = get_sen_coms(senate_committee)

    for senator in sen_dicts:
        if senator['link'] in sen_coms:
            senator['committees'] = sen_coms[senator['link']]['committees']

    legis_dicts = sen_dicts + house_dicts

    print('done getting dicts')

    with Pool() as pool:
        data = pool.map(scrape, legis_dicts)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
