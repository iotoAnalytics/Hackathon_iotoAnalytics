import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from datetime import datetime
import numpy as np
import re
import datetime
from multiprocessing import Pool
from unidecode import unidecode
from request_url import UrlRequest
from nameparser import HumanName
from scraper_utils import CAProvTerrLegislatorScraperUtils
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
import pandas as pd


scraper_utils = CAProvTerrLegislatorScraperUtils('ON', 'ca_on_legislators')
url = 'https://www.ola.org/en/members/current'
base_url = 'https://www.ola.org'
crawl_delay = scraper_utils.get_crawl_delay(url)
wiki_url = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Ontario'
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
current_year = datetime.datetime.now().year


def get_links(url):
    links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    rows = url_soup.find_all('tr')
    for item in rows:
        link = item.find('a').get("href")
        if "vacant" in link:
            pass
        elif 'members' in link:
            links.append(base_url + link)
    #print("number of members: " + str(len(links)))
    scraper_utils.crawl_delay(crawl_delay)
    return links


def get_wiki_links(url):
    base_wiki = 'https://en.wikipedia.org'
    wiki_links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table', {'class': 'wikitable sortable'})
    for item in url_table.find_all('tr'):
        try:
            wiki_links.append(base_wiki + item.find('span',
                              {'class': 'fn'}).find('a').get('href'))
        except Exception:
            pass
    return wiki_links


def get_info(soup):
    div_lst = soup.find('section', {'class': 'col-sm'}).find_all('div')
    content = soup.find_all('div', {'class': 'node__content'})
    com_lst = []
    party = ''
    current_term = ''
    address_lst = []
    phone_lst = []
    date_lst = []
    text = soup.find('div', {
        'class': 'view-display-id-member_role_history'}).find(
        'h3').text
    if 'present' in text:
        current_term = text.split('(')[0].strip()
    for item in soup.find_all('div', {'class': 'views-row'}):
        if item.find('h4'):
            date = re.search('\d{4}',
                             item.find('span', {'class': 'views-field views-field-field-start-date'}).text).group()
            for _ in range(int(date), current_year + 1):
                date_lst.append(_)
    for _ in range(2, 4):
        address_lst.append(
            {'location': content[_].find('h3').text, 'address': content[_].find('p').text.replace('\n', ' ')})
        try:
            phone = content[_].text.split('Tel.')[1].split('Fax')[0].replace('\n', '').strip().replace(' ', '-')
            phone_lst.append(
                {'office': content[_].find('h3').text, 'number': phone})
        except:
            pass
    for item in div_lst:
        if item.find('h2'):
            if item.find('h2').text == 'Current parliamentary roles':
                for el in item.find('div', {'class': "view-content"}).find_all('div', {'class': 'field-content'}):
                    com = el.text.split(',')
                    if len(com) == 1:
                        com_lst.append({'role': com[0], 'committee': ''})
                    elif len(com) == 2:
                        com_lst.append(
                            {'role': com[0], 'committee': com[1].strip()})
                    else:
                        com_lst.append(
                            {'role': com[0], 'committee': ' '.join(com[1:])})
            elif item.find('h2').text == 'Current party':
                temp = item.find(
                    'div', {'class': 'field-content'}).text.split('of')[0].strip()
                party = temp.replace('party', '').replace('Party', '').replace('Ontario', '').strip()

    date_lst = sorted(list(dict.fromkeys(date_lst)))
    return [com_lst, party, address_lst, phone_lst, date_lst, current_term]


def make_diction(legis_lst, wiki_lst):
    return_lst = []
    for item in legis_lst:
        return_lst.append({'url': item, 'wiki_list': wiki_lst})
    return return_lst


def scrape(diction):
    row = scraper_utils.initialize_row()
    row.source_url = diction['url']

    url_request = UrlRequest.make_request(diction['url'], header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    first_content = url_soup.find(
        'h1', {'class': 'field-content'}).text.split('(')
    info = get_info(url_soup)
    if len(first_content) == 2:
        name_full = ' '.join(first_content[0].split())
        riding = first_content[1].replace(')', '').strip()
    elif len(first_content) == 3:
        filler = first_content[0] + first_content[1]
        name_full = filler.replace(')', '').strip()
        riding = first_content[2].replace(')', '').strip()
    row.committees = info[0]
    row.party = info[1]
    try:
        row.party_id = scraper_utils.get_party_id(info[1])
    except:
        row.party_id = 0
    row.addresses = info[2]
    try:
        row.phone_numbers = info[3]
    except:
        print(row.name_full, row.source_url, row.phone_numbers)
    row.years_active = info[4]
    row.most_recent_term_id = info[5]

    hn = HumanName(name_full)
    name_first = hn.first
    name_middle = hn.middle
    name_last = hn.last
    row.name_full = name_full
    row.name_first = name_first
    row.name_middle = name_middle
    row.name_last = name_last

    row.riding = riding
    email = url_soup.find('div', {
        'class': 'field field--name-field-email-address field--type-email field--label-hidden field__items'}).text.replace(
        '\n', '')
    row.email = email

    uClient = uReq('https://en.wikipedia.org/wiki/Legislative_Assembly_of_Ontario')
    page_html = uClient.read()
    uClient.close()
    page_soup = BeautifulSoup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        name_td = tr.findAll("td")[1]
        name = name_td.text
        district = tr.findAll("td")[3].text
        if unidecode(row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(row.name_last.lower()) in unidecode(name.strip().lower()):
            row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
            bio = get_biography_from_wiki(row.wiki_url)
            row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio)
            break

    print('Done row for ' + name_full)
    scraper_utils.crawl_delay(crawl_delay)
    return row

def get_biography_from_wiki(link):
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    page_soup = BeautifulSoup(page_html, "html.parser")
    main_content = page_soup.find("div", {"id" : "content"}).text
    return main_content


if __name__ == '__main__':
    # pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_columns', None)

    legislator_links = get_links(url)
    wiki_links = get_wiki_links(wiki_url)
    dict_lst = make_diction(legislator_links, wiki_links)

    print('done making dict lists')
    with Pool() as pool:
        mla_data = pool.map(scrape, dict_lst)

    leg_df = pd.DataFrame(mla_data)
    # drop columns that we'll get from wikipedia instead
    leg_df = leg_df.drop(columns=['birthday', 'education', 'occupation'])

    with Pool() as pool:
        wiki_data = pool.map(
            func=scraper_utils.scrape_wiki_bio, iterable=wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'wiki_url', 'occupation']
    ]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["wiki_url"])
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})

    big_list_of_dicts = big_df.to_dict('records')
    print('done collecting data')
    scraper_utils.write_data(big_list_of_dicts)
    print('complete!')
