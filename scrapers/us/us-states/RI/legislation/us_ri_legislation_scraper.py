from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
from nameparser import HumanName
import re
import sys
import os
from pathlib import Path
from request_url import UrlRequest

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

state_abbreviation = 'RI'
scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, 'us_ri_legislation', 'us_ri_legislators')

text_url = 'http://webserver.rilegislature.gov/BillText21/'
base_url = 'http://webserver.rilegislature.gov'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/87.0.4280.88 Safari/537.36'}
session = f'20{re.search(r"[0-9]{2}", text_url).group()}'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def calender_month(key):
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
    }[key]


def get_bill_text_links(url, type):
    links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table', {'class': 'bill_data'}).find_all('tr')
    for item in url_table:
        try:
            bill_name = item.find('td', {'class': 'bill_col1'}).text
            bill_link = item.find('td', {'class': 'bill_col3'}).find('a').get('href')
            links.append({
                'bill_name': bill_name,
                'link': f'http://webserver.rilegislature.gov/BillText21/{type}Text21/' + bill_link
            })
        except:
            pass
    return links


def get_links(url):
    return_lst = []
    links = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_summary = url_soup.find_all('a')
    for item in url_summary:
        if 'HouseText' in item.get('href') or 'SenateText' in item.get('href'):
            links.append(base_url + item.get('href'))
    for link in links:
        if 'Senate' in link:
            return_lst += get_bill_text_links(link, 'Senate')
        elif 'House' in link:
            return_lst += get_bill_text_links(link, 'House')
    return return_lst


def text_edit(txt):
    return txt.replace('\n', ' ').replace('\r', '').replace('\xa0', '').strip()


def edit_sponsors(sponsors):
    text = sponsors.replace('Representatives', '')
    text = text.replace('Representative', '')
    text = text.replace('Senators', '')
    text = text.replace('Senator', '')
    text = text.replace(' and ', ' ').strip()
    if ',' in text:
        text = text.split(',')
        text = [x.strip() for x in text]
    return text


def edit_date(date):
    date_lst = date.strip().split(' ')
    month = calender_month(date_lst[0].strip())
    day = date_lst[1].replace(',', '').strip()
    year = date_lst[2].strip()
    return f'{year}-{month}-{day}'


def scrape_info(item):
    url_request = UrlRequest.make_request(item['link'], header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    explain = ''
    bill_text = url_soup.text

    row = scraper_utils.initialize_row()
    row.bill_text = text_edit(bill_text)
    for el in url_soup.find_all('tr'):
        td = el.find('td', {'class': 'TD_TEXT'}).text
        if 'Introduced By' in td:
            leg_type = ''
            if 'Senator' in td:
                leg_type = 'Senator'
            elif 'Representative' in td:
                leg_type = 'Representative'
            sponsors = edit_sponsors(td.split(':')[1])
            if type(sponsors) == str:
                row.principal_sponsor = sponsors

                hn = HumanName(sponsors)
                name_first = hn.first
                name_last = hn.last

                if '-' in name_first:
                    name_first = name_first.split('-')[0]

                if len(name_first) > 1:

                    search_for = dict(name_first=name_first, name_last=name_last, role=leg_type)

                    if name_first.title()== 'Shallcross':
                        search_for = dict(name_middle='Ann Shallcross', role=leg_type)

                    row.principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)
                elif len(name_first) == 1:
                    search_for = dict(name_last=name_last, role=leg_type)

                    row.principal_sponsor_id = scraper_utils.legislators_search_startswith(
                        'goverlytics_id', 'name_first', name_first, **search_for
                    )

            # add leg_type search param here
            elif type(sponsors) == list:
                row.sponsors = sponsors
                sponsor_ids = []
                for sponsor in sponsors:
                    if ' ' in sponsor.strip():
                        duped_name = sponsor.split()
                        search_for = dict(name_last=duped_name[1], role=leg_type)
                        sponsor_id = scraper_utils.legislators_search_startswith(
                            'goverlytics_id', 'name_first', duped_name[0].upper(), **search_for)
                    else:
                        search_for = dict(name_last=sponsor, role=leg_type)
                        if sponsor.title() == 'Shallcross':
                            search_for = dict(name_middle='Ann Shallcross', role=leg_type)
                        sponsor_id = scraper_utils.get_legislator_id(**search_for)
                    sponsor_ids.append(sponsor_id)
                row.sponsors_id = sponsor_ids

        elif 'Date Introduced' in td:
            date = td.split(':')[1]
            row.date_introduced = edit_date(date)
        elif el.find('p', {'class': 'EXPLAIN'}):
            text = el.find('p', {'class': 'EXPLAIN'}).text
            text = text.replace('\xa0', '')
            explain += text

    link = item['link']
    if 'Bill' in link and 'Senate' in link:
        row.chamber_origin = 'Senate'
        row.bill_type = 'Bill'
    elif 'Bill' in link and 'House' in link:
        row.chamber_origin = 'House'
        row.bill_type = 'Bill'
    bill_name = item['bill_name']
    row.goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    row.session = session
    row.source_url = link
    row.bill_name = bill_name
    row.bill_summary = explain.strip()
    print(f'done row for {bill_name}')
    return row


if __name__ == '__main__':
    bill_info = get_links(text_url)

    with Pool() as pool:
        data = pool.map(scrape_info, bill_info)

    scraper_utils.write_data(data)

    print('Complete!')
