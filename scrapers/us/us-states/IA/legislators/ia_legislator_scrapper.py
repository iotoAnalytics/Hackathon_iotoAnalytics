'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from multiprocessing import Pool
import pandas as pd
from nameparser import HumanName
import re

state_abbreviation = 'IA'
database_table_name = 'us_ia_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

house_url = 'https://www.legis.iowa.gov/legislators/house'
sen_url = 'https://www.legis.iowa.gov/legislators/senate'
base_url = 'https://www.legis.iowa.gov'
wiki_url = 'https://en.wikipedia.org/wiki/Iowa_General_Assembly'
wiki_base = 'https://en.wikipedia.org'

# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_info_table_info(soup):
    dictionary = {}
    table = soup.find('div', {'class': 'legisIndent divideVert'})
    info_table = pd.read_html(str(table))[0]
    #     print(info_table)
    for item in info_table.to_dict('records'):
        if item[0] == 'Occupation:':
            dictionary['occupation'] = item[1].split(',')
        elif item[0] == 'Capitol Phone:':
            dictionary['phone_number'] = {
                'office': '',
                'number': item[1].replace('.', '-')
            }
    return dictionary


def get_committees(soup):
    coms = []
    for item in soup.find_all('ul', {'class': 'bullet tight legisIndent'}):
        for el in item.find_all('a'):
            if '(' in el.text:
                text = el.text.split('(')
                coms.append({
                    'role': text[1].replace(')', '').strip(),
                    'committee': text[0].strip()
                })
            else:
                coms.append({
                    'role': 'member',
                    'committee': el.text.strip()
                })
    return coms


def get_years(soup):
    years_active = []
    years_html = soup.find('ul', {'class': 'selectionList'}).find_all('li')
    for item in years_html:
        years_range = item.find('span', {'class': 'r'}).text
        years_text = [int(x) for x in re.findall('[0-9]{4}', years_range)]
        years_list = list(range(years_text[0], years_text[1] + 1))
        for el in years_list:
            if el not in years_active:
                years_active.append(el)
    return sorted(years_active)


def get_wiki_info(name_first, name_last, chamber):
    page = scraper_utils.request(wiki_url)
    soup = BeautifulSoup(page.content, 'lxml')
    if chamber == 'House':
        table = soup.find('div', {'aria-labelledby': 'Members_of_the_Iowa_House_of_Representatives'})
    elif chamber == 'Senate':
        table = soup.find('div', {'aria-labelledby': 'Members_of_the_Iowa_Senate'})
    for item in table.find('div', {'class': 'div-col'}).find_all('li'):
        if name_first in item.text and name_last in item.text and 'does not exist' not in item.get('title'):
            page = scraper_utils.request(wiki_base + item.find('a').get('href'))
            scraper_utils.crawl_delay(crawl_delay)
            return scraper_utils.scrape_wiki_bio(page)
        else:
            pass


def get_dict(url):
    lst = []
    url_requests = scraper_utils.request(url).content
    url_soup = BeautifulSoup(url_requests, 'lxml')
    url_table = url_soup.find('table', {'id': 'sortableTable'})
    info_lst = pd.read_html(str(url_table))[0].to_dict('records')
    for item in url_table.find_all('tr'):
        try:
            lst.append(base_url + item.find('a').get('href'))
        except:
            pass
    if len(lst) == len(info_lst):
        for _ in range(0, len(lst)):
            info_lst[_]['link'] = lst[_]
        return info_lst
    else:
        print('Lengths of lists do not match!')
        return None


def scrape(dict_item):
    row = scraper_utils.initialize_row()

    name_full = dict_item['Name']
    hn = HumanName(name_full)
    name_first = hn.first
    name_middle = hn.middle
    name_last = hn.last

    row.name_full = name_full.title()
    row.name_first = name_first.title()
    row.name_middle = name_middle.title()
    row.name_last = name_last.title()

    row.district = dict_item['District']
    row.party = dict_item['Party']
    row.party_id = scraper_utils.get_party_id(dict_item['Party'])
    row.email = dict_item['Email']
    row.areas_served = [x.strip() for x in dict_item['County'].split(',')]

    if dict_item['Chamber'] == 'House':
        row.role = 'Representative'
    elif dict_item['Chamber'] == 'Senate':
        row.role = 'Senator'

    link = dict_item['link']
    row.source_url = link
    row.state_member_id = link.split('personID=')[1]
    row.most_recent_term_id = link.split('?')[1].split('=')[1].split('&')[0]

    page = scraper_utils.request(link)
    soup = BeautifulSoup(page.content, 'lxml')
    info_table_dict = get_info_table_info(soup)
    if 'occupation' in info_table_dict:
        row.occupation = info_table_dict['occupation']
    if 'phone_number' in info_table_dict:
        row.phone_numbers = info_table_dict['phone_number']

    row.committees = get_committees(soup)
    row.years_active = get_years(soup)
    try:
        wiki_info = get_wiki_info(name_first, name_last, dict_item['Chamber'])
        row.birthday = wiki_info['birthday']
        row.education = wiki_info['education']
    except Exception:
        pass

    scraper_utils.crawl_delay(crawl_delay)
    print(f'Done row for {name_first}')
    return row


if __name__ == '__main__':
    urls = get_dict(house_url) + get_dict(sen_url)

    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
