import pandas as pd
import bs4
from urllib.request import Request
from bs4 import BeautifulSoup
import psycopg2
from nameparser import HumanName
from request_url import UrlRequest
import requests
import datefinder
import unidecode
from multiprocessing import Pool
import datetime
import re
import numpy as np
from datetime import datetime
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
from legislation_scraper_utils import CAProvinceTerrLegislationScraperUtils

url = 'https://www.ola.org/en/legislative-business/bills/current'
base_url = 'https://www.ola.org'
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}

prov_terr_abbreviation = 'ON'
database_table_name = 'ca_on_legislation'
legislator_table_name = 'ca_on_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def month_to_num(short_month):
    return {
        'January': '01',
        'February': '02',
        'March': '03',
        'April': '04',
        'May': '05',
        'June': '06',
        'July': '07',
        'August': '08',
        'September': '09',
        'October': '10',
        'November': '11',
        'December': '12'
    }[short_month]


def fix_sponsors(sponsor_lst):
    return_lst = []
    for item in sponsor_lst:
        hn = HumanName(item)
        return_lst.append(hn.first + ' ' + hn.last)
    return return_lst


def edit_date(date_item):
    date = date_item.replace(',', '').split(' ')
    month = month_to_num(date[0])
    if int(date[1]) < 10:
        day = '0' + date[1]
    else:
        day = date[1]
    return date[2] + '-' + month + '-' + day


def edit_table(lst):
    return_lst = []
    committees = []
    for item in lst:
        date = edit_date(item['Date'])
        if item['Committee'] == '-':
            action_by = ''
        else:
            action_by = item['Committee']
        description = item['Bill stage']
        coms = {'chamber': '', 'committee': action_by}
        committees.append(coms)
        diction = {'date': date, 'action_by': action_by, 'description': description}
        return_lst.append(diction)
    return [return_lst, coms]


def get_bill_links(url):
    link_dict = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_content = url_soup.find('div', {'class': 'view-content'})
    for item in url_content.find_all('div', {'class': 'views-row'}):
        link = base_url + item.find('a').get('href')
        sponsors_html = item.find_all('div', {'class': 'node__content'})
        sponsors = []
        for el in sponsors_html:
            sponsor = el.find('div', {
                'class': 'field field--name-field-full-name-by-last-name field--type-string field--label-hidden d-inline-block field__item'}).text
            sponsors.append(sponsor)
        sponsors = fix_sponsors(sponsors)
        table_html = item.find('table')
        pd_table = pd.read_html(str(table_html))[0]
        table = edit_table(pd_table.to_dict('records'))
        actions = table[0]
        committees = table[1]
        link_dict.append({'url': link, 'sponsors': sponsors, 'actions': actions, 'committees': committees})
    scraper_utils.crawl_delay(crawl_delay)
    return link_dict


def scrape(dict_item):
    url = dict_item['url']
    session = re.search('parliament-[1-9]{2}', url).group().title()
    sponsors = dict_item['sponsors']
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')

    row = scraper_utils.initialize_row()
    row.source_url = url
    row.actions = dict_item['actions']
    row.committees = dict_item['committees']
    row.session = session

    if len(sponsors) == 1:
        row.principal_sponsor = sponsors[0]
        names = sponsors[0].split(' ')
        row.principal_sponsor_id = scraper_utils.get_legislator_id(name_first=names[0], name_last=names[1])
    else:
        row.sponsors = sponsors
        sponsor_id_lst = []
        for sponsor in sponsors:
            names = sponsor.split(' ')
            sponsor_id = scraper_utils.get_legislator_id(name_first=names[0], name_last=names[1])
            sponsor_id_lst.append(sponsor_id)
        row.sponsors_id = sponsor_id_lst

    bold_header = url_soup.find('div', {'class': 'view-header'}).text.replace('\n', '').split(',')
    bill_name = bold_header[0].replace(' ', '').strip()
    bill_title = bold_header[1].strip()
    current_status = url_soup.find('div', {'class': 'list-inline-item nav-item'}).text.replace('Current status: ',
                                                                                               '').replace('\n', '')
    bill_summary = url_soup.find('p', {'class': 'section'}).text.replace('\r', ' ').replace('\n', ' ')
    bill_text = url_soup.find('div', {'class': 'WordSection1'}).text.replace('\n', ' ').replace('\r', ' ').replace(
        '\xa0', '').strip()
    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'

    row.goverlytics_id = goverlytics_id
    row.bill_name = bill_name
    row.bill_title = bill_title
    row.current_status = current_status
    row.bill_summary = bill_summary
    row.bill_text = bill_text
    scraper_utils.crawl_delay(crawl_delay)
    print('Done row for: '+bill_name)
    return row


if __name__ == '__main__':
    link_dicts = get_bill_links(url)
    print('Done making dicts!')
    with Pool() as pool:
        data = pool.map(scrape, link_dicts)
    print('Done scraping!')
    scraper_utils.insert_legislation_data_into_db(data)
    print('Complete!')
