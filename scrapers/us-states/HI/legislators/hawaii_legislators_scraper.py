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
import request_url
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3

# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

# state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
# database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
# country = str(configParser.get('scraperConfig', 'country'))

scraper_utils = USStateLegislatorScraperUtils('HI', 'us_hi_legislators')

current_year = 2021
rep_wiki_url = 'https://en.wikipedia.org/wiki/Hawaii_House_of_Representatives'
sen_wiki_url = 'https://en.wikipedia.org/wiki/Hawaii_Senate'
base_url = 'https://www.capitol.hawaii.gov/members/legislators.aspx?chamber=all'
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.88 Safari/537.36'}


def get_name(soup):
    name = soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_LabelLegname'}).text
    name = name.replace('\xa0', ' ').replace(',', '').strip()
    temp_name = name.split(' ')
    f_name = temp_name[0]
    l_name = temp_name[-1].title().strip()
    m_name = name.replace(f_name, '').replace(l_name, '').strip()
    if l_name == 'Jr.':
        l_name = m_name
        m_name = ''
    return [name, f_name, l_name, m_name]


def get_role(soup):
    role = soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_LabelSenRep'}).text
    return role


def get_district(soup):
    district = soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_LabelDistrict'}).text
    return district


def get_phone(soup):
    phone = soup.find('span', {'id': 'ctl00_ContentPlaceHolderCol1_LabelPhone'}).text
    box_text = soup.find('div', {'class': 'roundedrect gradientgray shadow'}).text.replace('\r', '').split('\n')
    box_text = [x for x in box_text if x]
    return {'office': box_text[3].strip(), 'number': phone}


def get_rep_wikilink(soup):
    wiki_req = request_url.UrlRequest.make_request(rep_wiki_url, header)
    wiki_soup = BeautifulSoup(wiki_req.content, 'lxml')
    rep_table = wiki_soup.find('table', {'class': 'sortable wikitable'}).find_all('a')
    url = None
    for item in rep_table:
        if get_name(soup)[1] and get_name(soup)[2] in item.text:
            url = 'https://en.wikipedia.org' + item.get('href')
    return url


def get_sen_wikilink(soup):
    wiki_req = request_url.UrlRequest.make_request(sen_wiki_url, header)
    wiki_soup = BeautifulSoup(wiki_req.content, 'lxml')
    sen_table = wiki_soup.find('table', {'class': 'wikitable sortable'}).find_all('a')
    url = None
    for item in sen_table:
        if get_name(soup)[1] and get_name(soup)[2] in item.text:
            url = 'https://en.wikipedia.org' + item.get('href')
    return url


# does not scrape fields bc all the legislators I've screened dont have that; might need change later on
def get_education(url):
    dict_list = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table', attrs={'class': 'infobox vcard'})
    test_lst = url_table.find_all('tr')
    for item in test_lst:
        try:
            if 'Education' in item.text or 'Alma mater' in item.text:
                temp = item.text.replace('Education', '').replace('Alma mater', '').split(')')
                for elem in temp:
                    if elem != '':
                        uni_info = elem.split('(')
                        dict_list.append({'level': uni_info[1].strip(), 'field': '', 'school': uni_info[0].strip()})
        except:
            pass

    return dict_list


def scrape_wiki(link):
    years_active = []
    bday = None
    job = []
    education = []
    if link is None or 'action=edit&redlink=1' in link:
        pass
    else:
        wiki_req = request_url.UrlRequest.make_request(link, header)
        wiki_soup = BeautifulSoup(wiki_req.content, 'lxml')
        rep_table = wiki_soup.find('table', {'class': 'infobox vcard'})
        education = get_education(link)

        try:
            for item in rep_table.find_all('td'):
                if 'office' in item.text:
                    temp = re.findall('\d{4}', item.text)
                    if len(temp) == 2:
                        temp_lst = [x for x in range(int(sorted(temp)[0]), int(sorted(temp)[1]) + 1)]

                        years_active = years_active + temp_lst

                    elif len(temp) == 1:
                        years_active.append(int(temp[0]))
            for item in rep_table.find_all('tr'):
                if 'Born' in item.text:
                    bday = item.find('span', {'class': 'bday'}).text
                if 'Profession' in item.text:
                    job.append(item.text.replace('Profession', ''))
        except AttributeError:
            pass
        years_active = sorted(years_active)
        try:
            years_active = years_active + [x for x in range(years_active[-1] + 1, current_year + 1)]
        except IndexError:
            pass
    return [bday, years_active, job, education]


def get_email(soup):
    email = soup.find('a', {'id': 'ctl00_ContentPlaceHolderCol1_HyperLinkEmail'}).text
    return email


def get_com(soup, name):
    com_items = []
    try:
        com_soup = soup.find('table', {'id': 'ctl00_ContentPlaceHolderCol1_GridViewMemberof'}).find_all('tr')
        for item in com_soup:
            com = item.text.replace('\n', '')
            link = 'https://www.capitol.hawaii.gov' + item.find('a').get('href')
            link_request = request_url.UrlRequest.make_request(link, header)
            link_soup = BeautifulSoup(link_request.content, 'lxml')
            chair = link_soup.find('a', {'id': 'ctl00_ContentPlaceHolderCol1_HyperLinkChair'}).text.strip()
            vice_chair = link_soup.find('a', {'id': 'ctl00_ContentPlaceHolderCol1_HyperLinkcvChair'}).text.strip()
            if name.strip() == chair:
                role = 'Chair'
            elif name.strip() == vice_chair:
                role = 'Vice Chair'
            else:
                role = 'Member'
            com_items.append({'Role': role, 'Committee': com})
    except AttributeError:
        pass
    return com_items


def get_dicts(url):
    links = []
    url_request = request_url.UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_tr = url_soup.find('table',{'id':'ctl00_ContentPlaceHolderCol1_GridView1'}).find_all('tr')
    for item in url_tr:
        try:
            link = 'https://www.capitol.hawaii.gov'+item.find('a').get('href')
            party = re.search('\([A-Z]\)',item.text).group()
            if party == '(D)':
                party = 'Democrat'
            elif party == '(R)':
                party = 'Republican'
            areas = item.find_all('td')[-1].text.replace('\n','')
            areas = re.sub('[A-Z]District[0-9]*','',areas).split(',')
            areas_lst = [x.strip().replace('\x80\x98','').replace('â','') for x in areas]
            links.append({'url':link,'party':party,'areas':areas_lst})
        except AttributeError:
            pass
    return links


def scrape(lst_item):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    url = lst_item['url']
    row.source_url = url
    row.party = lst_item['party']
    if lst_item['party'] == 'Democrat' or lst_item['party'] == 'Republican':
        row.party_id = scraper_utils.get_party_id(lst_item['party'])
    row.areas_served = lst_item['areas']

    soup_req = request_url.UrlRequest.make_request(url, header)
    soup = BeautifulSoup(soup_req.content, 'lxml')

    name = get_name(soup)
    row.name_full = name[0]
    row.name_first = name[1]
    row.name_last = name[2]
    row.name_middle = name[3]

    role = get_role(soup)
    row.role = role
    if role == 'Senator':
        wiki_info = scrape_wiki(get_sen_wikilink(soup))
    elif role == 'Representative':
        wiki_info = scrape_wiki(get_rep_wikilink(soup))
    row.birthday = wiki_info[0]
    row.years_active = wiki_info[1]
    row.occupation = wiki_info[2]
    row.education = wiki_info[3]
    row.email = get_email(soup)
    row.district = get_district(soup)
    row.phone_number = get_phone(soup)
    row.committees = get_com(soup, name[0])
    print('done row for: ' + name[0])
    return row


# FIX GET_EDUCATION BEFORE RUNNING PLS
if __name__ == '__main__':
    urls = get_dicts(base_url)
    print('done getting dicts!')

    with Pool() as pool:
        data = pool.map(scrape, urls)
    print('done scraping!')

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
