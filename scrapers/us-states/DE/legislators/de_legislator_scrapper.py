'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from legislator_scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from request_url import UrlRequest
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name, country)

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/87.0.4280.88 Safari/537.36'}

# using these links bc I couldn't see the html properly from scraping the actual legislator page
url_rep = 'https://legis.delaware.gov/Chambers/House/District'
url_sen = 'https://legis.delaware.gov/Chambers/Senate/SenateDistrict'

url_base = 'https://legis.delaware.gov'
wiki_link = 'https://en.wikipedia.org/wiki/Delaware_General_Assembly'

current_year = 2021


def get_wiki_links(url):
    lst = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    tables = url_soup.find_all('div', {'class': 'div-col', 'style': 'column-width: 18em;'})
    for item in tables:
        for el in item.find_all('li'):
            lst.append('https://en.wikipedia.org' + el.find('a').get('href'))
    return lst


def split_name(name):
    temp = name.split(' ')
    f_name = temp[0]
    l_name = temp[-1]
    m_name = name.replace(f_name, '').replace(l_name, '').strip()
    if m_name == 'Elizabeth' and f_name == 'S.':
        m_name = 'Tizzy'
        f_name = 'Elizabeth'
    elif m_name == 'S Postles' and l_name=='Jr.':
        l_name = 'Postles'
        m_name = ''
    return [f_name, l_name, m_name]


def find_wiki_link(name, wiki_list):
    name_split = split_name(name)
    f_name = name_split[0]
    l_name = name_split[1]
    wiki = None
    for item in wiki_list:
        if f_name in item and l_name in item:
            wiki = item
    return wiki


def get_links(url, wiki_list):
    role = 'Senator' if 'Senate' in url else 'Representative'
    lst = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    district_lst = url_soup.find_all('div', {'class': 'col-xs-24 col-sm-12 col-md-24 col-lg-12 distlinks'})
    for html in district_lst:
        district_items = html.find_all('a', {'class': 'district-item'})
        for item in district_items:
            name = item.text.replace('\n', ' ').strip()
            party = re.search('\([A-Z]\)', name).group()
            district = item.get('data-districtnumber')
            link = item.get('href')
            state_id = link.replace('/LegislatorDetail/', '')
            name = name.replace(party, '').strip()
            wiki = find_wiki_link(name, wiki_list)
            if party == '(D)':
                party = 'Democrat'
            elif party == '(R)':
                party = 'Republican'
            else:
                party = None
            # This is necessary bc the link for this legislator is wrong on the website... Delaware website be scuffed
            if name == 'Michael Ramone' and district == '21':
                link = '/LegislatorDetail/243'
                state_id = '243'
            elif name == 'Marie Pinkey' and district == '13':
                name = 'Marie Pinkney'
            district_dict = {'name': name, 'district': district, 'id': state_id,
                             'link': link, 'party': party,
                             'wiki': wiki, 'role': role}
            lst.append(district_dict)
    return lst


def get_phone(soup, location):
    phone = soup.find('div', {'class': 'info-phone'}).text.replace('\n', '').split(':')[1]
    return {'office': location, 'number': phone}


def get_address(soup, location):
    info_value = soup.find_all('div', {'class': 'info-value'})
    leg_hall = info_value[1].text.replace('\n', '').replace('\r', '')
    address = ''
    for elem in [x.strip() for x in leg_hall.split(',')]:
        address += ' ' + elem
    address = address.strip()
    address = info_value[0].text + ',' + address
    return [{'location': location, 'address': address}]


def get_com(soup):
    com_lst = []
    for item in soup.find_all('div', {'class': 'row'}):
        try:
            if 'Committees' in item.find('h3', {'class': 'section-head'}).text:
                for el in item.find_all('li'):
                    com = el.text.split(',')
                    com_dict = {'role': com[1].strip(), 'committee': com[0].strip()}
                    com_lst.append(com_dict)
        except AttributeError:
            pass
    return com_lst


def get_years(soup):
    lst = []
    for item in soup.find_all('div', {'class': 'col-xs-24'}):
        try:
            if 'Legislative Service' == item.find('h3').text:
                lst.append(item)
        except:
            pass
    is_present = lst[1].text
    try:
        if len(is_present.split(',')) == 2:
            years_text = lst[1].text.strip()
            if ',' in years_text:
                years_text = years_text.split(',')[1]
            years_range = re.findall('[0-9]{4}', years_text)
            if ('Present' in is_present or 'present' in is_present) and len(years_range) == 1:
                first_year = int(years_range[0])
                years_active = [x for x in range(first_year, int(current_year))] + [current_year]
            elif len(years_range) > 1:
                years_active = [x for x in range(int(years_range[0]), int(years_range[-1]))] + [int(years_range[-1])]
            else:
                years_active = current_year
            return years_active

        elif len(is_present.split(',')) > 2:
            years = []
            for item in is_present.split(','):
                if re.findall('[0-9]{4}', item):
                    years += re.findall('[0-9]{4}', item)
            years = [int(x) for x in list(dict.fromkeys(years))]
            years.sort()
            if 'Present' in is_present or 'present' in is_present:
                years_active = [x for x in range(years[0], current_year)] + [current_year]
            else:
                years_active = [x for x in range(years[0], years[-1])] + [years[-1]]
            return years_active

    except Exception as e:
        print(e)
        return None


def educated(edu_string):
    if 'University' in edu_string or 'College' in edu_string or 'School' in edu_string:
        return True
    else:
        return False


def scrape_wiki_link(url):
    edu = []
    job = []
    bday = None
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    info_box = url_soup.find('table', {'class': 'infobox vcard', 'style': 'width:22em'})
    for item in info_box.find_all('tr'):
        if 'Alma mater' in item.text or 'Education' in item.text:
            # print(unis)
            if ')' and '(' in item.text:
                # print(re.findall('\(.*?\)', item.text))
                edu_text = item.text.replace('Alma mater', '').replace('Education', '')
                edu_list = [x.strip() for x in edu_text.split(')') if x]
                for el in edu_list:
                    split_el = el.split('(')
                    edu_dict = {'level': split_el[1], 'field': '', 'school': split_el[0].strip()}
                    edu.append(edu_dict)
            else:
                unis = [x.get('title') for x in item.find_all('a') if educated(x.text)]
                for el in unis:
                    edu_dict = {'level': '', 'field': '', 'school': el}
                    edu.append(edu_dict)
        if 'Profession' in item.text:
            job = item.text.replace('Profession', '').split(',')
        if 'Born' in item.text:
            bday = item.find('span', {'class': 'bday'}).text
    return [edu, job, bday]


def scrape(legis_dict):
    row = scraper_utils.initialize_row()

    link = url_base + legis_dict['link']
    url_request = UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    location = url_soup.find('h6').text
    name_split = split_name(legis_dict['name'])

    row.name_full = legis_dict['name']
    row.name_first = name_split[0]
    row.name_last = name_split[1]
    row.name_middle = name_split[2]
    row.district = legis_dict['district']
    row.state_member_id = legis_dict['id']
    row.source_url = link
    row.party = legis_dict['party']
    row.party_id = scraper_utils.get_party_id(legis_dict['party'])
    row.role = legis_dict['role']

    row.phone = get_phone(url_soup, location)
    row.email = url_soup.find_all('div', {'class': 'info-group'})[1].find('a').text
    row.addresses = get_address(url_soup, location)
    row.committees = get_com(url_soup)
    row.years_active = get_years(url_soup)

    try:
        wiki_info = scrape_wiki_link(legis_dict['wiki'])
        row.education = wiki_info[0]
        row.occupation = wiki_info[1]
        row.birthday = wiki_info[2]
    except:
        pass

    print('done row for: ' + legis_dict['name'])
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    wiki = get_wiki_links(wiki_link)
    legis_info = get_links(url_rep, wiki) + get_links(url_sen, wiki)
    print('done dicts!')

    with Pool() as pool:
        data = pool.map(scrape, legis_info[0:5])
    print('done scraping!')
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
