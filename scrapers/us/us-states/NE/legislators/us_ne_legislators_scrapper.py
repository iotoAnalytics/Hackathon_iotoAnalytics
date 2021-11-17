import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import pandas as pd
from datetime import date
from database import Database
from pprint import pprint
from nameparser import HumanName
import numpy as np
import re
import boto3
import time
from urllib.request import urlopen as uReq
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


state_abbreviation = 'NE'
database_table_name = 'us_ne_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'http://news.legislature.ne.gov/'
senate_url = 'https://nebraskalegislature.gov/senators/senator_list.php'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

current_year = date.today().year

wiki_url = 'https://en.wikipedia.org/wiki/Nebraska_Legislature'
wiki_soup = BeautifulSoup(requests.get(wiki_url).content, 'lxml')
wiki_dict = pd.read_html(str(wiki_soup.find('table', {'class':'wikitable sortable'})))[0].to_dict('records')

test_lst = wiki_soup.find('table', {'class':'wikitable sortable'}).find('tbody').find_all('tr')[1:]
wiki_link_lst = []
for item in test_lst:
    link = item.find('a').get('href')
    if 'redlink=1' not in link:
        hN = HumanName(item.find('a').text)
        first = hN.first
        last = hN.last
        wiki_link_lst.append({
            'first': first,
            'last': last,
            'link': 'https://en.wikipedia.org'+link
        })

for item in wiki_link_lst:
    for el in wiki_dict:
        if item['first'] in el['Senator'] and item['last'] in el['Senator']:
            el['link'] = item['link']


def get_urls(sen_url):
    soup = BeautifulSoup(requests.get(sen_url).content, 'lxml')
    sen_links = soup.find('ul', {'class': 'list-group dist_list'}).find_all('li')
    sen_links = [x.find('a').get('href') for x in sen_links if 'dist' in x.find('a').get('href')]
    scraper_utils.crawl_delay(crawl_delay)
    return sen_links


def get_committees(soup):
    block_box_soup = soup.find('div', {'class':'col-sm-8 col-md-4 col-lg-3'}).find_all('div', {'class':'block-box'})
    for block_box in block_box_soup:
        try:
            if block_box.find('div', {'class':'feature-heading'}).text.strip() == 'Committee Assignments':
                com_lst = []
                for x in block_box.find('ul').childGenerator():
                    if str(x) != '<br/>' and str(x) != ' ':
                        com_lst.append({
                            'role': 'member',
                            'committee': str(x).replace('&comma;', '').strip()
                        })
                return com_lst
        except AttributeError:
            pass


def get_legis_info(link):
    soup = BeautifulSoup(requests.get(link).content, 'lxml')
    info_box = soup.find('div', {'class':'block-box'})
    name = info_box.find('h2').text.replace('Sen.', '')
    district = re.search('[0-9]+', link).group()
    if district[0] == '0':
        district = district[1:]
    address_box = info_box.find('address', {'class':'feature-content'}).text
    adbox_info = address_box.split('(')
    email = info_box.find('address', {'class':'feature-content'}).find('a').text
    address = adbox_info[0].replace('\n', '').split()
    address = ' '.join([x for x in address if x])
    phone = '(' + adbox_info[1].split('Email')[0].replace('\n', '').strip()
    phone = phone.replace('(', '').replace(')', '').replace(' ', '-')
    coms = get_committees(soup)
    info_dict = {
        'name': name.strip(),
        'district':district,
        'address': address,
        'email': email,
        'phone': phone,
        'coms': coms
    }
    return info_dict


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("table")
    rows = tables[3].findAll("tr")

    for person in rows[1:]:
        info = person.findAll("td")
        try:
            biolink = info[1].a["href"]

            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def get_wiki_url(row):

    wikipage_senate = "https://ballotpedia.org/Nebraska_State_Senate_(Unicameral)"

    if row.role == "Senator":

        try:
            uClient = uReq(wikipage_senate)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()

                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
            if not row.wiki_url:
                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()

                    if party == "Democratic":
                        party = "Democrat"

                    if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
        except Exception as e:
            print(e)
            pass


def scrape(url):
    row = scraper_utils.initialize_row()

    info_dict = get_legis_info(url)
    hn = HumanName(info_dict['name'])
    row.name_full = info_dict['name']
    row.name_first = hn.first
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.district = info_dict['district']
    row.address = info_dict['address']
    row.source_url = url
    row.addresses = [{
        'location': 'district office',
        'address': info_dict['address']
    }]
    row.phone_numbers = [{
        "office": "district office",
        "number": info_dict['phone']
    }]
    row.email = info_dict['email']
    row.committees = info_dict['coms']
    row.role = 'Senator'

    for wiki_el in wiki_dict:
        if hn.first in wiki_el['Senator'] and hn.last in wiki_el['Senator']:
            party = wiki_el['Party affiliation'].replace('tic', 't')
            row.party = party
            row.party_id = scraper_utils.get_party_id(party)
            row.years_active = [x for x in range(int(wiki_el['Took office'][:4]), current_year + 1)]
            if 'link' in wiki_el:
                wiki_info = scraper_utils.scrape_wiki_bio(wiki_el['link'])
                row.birthday = wiki_info['birthday']
                row.education = wiki_info['education']
                row.occupation = wiki_info['occupation']
                row.most_recent_term_id = wiki_info['most_recent_term_id']
                scraper_utils.crawl_delay(crawl_delay)

    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender
    get_wiki_url(row)
    scraper_utils.crawl_delay(crawl_delay)
    print(f"done row for {info_dict['name']}")
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    print('Getting urls...')
    urls = get_urls(senate_url)
    print('Scraping...')
    with Pool() as pool:
        data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)

    wikipage_senate = 'https://ballotpedia.org/Nebraska_State_Senate_(Unicameral)'

    all_wiki_links = find_individual_wiki(wikipage_senate)

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_first', 'name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['wiki_url'] = big_df['wiki_url'].replace({np.nan: None})

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
