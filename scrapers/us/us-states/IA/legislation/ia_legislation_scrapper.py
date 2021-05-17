import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from selenium import webdriver
import time
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3

sleep_time = 1.5

state_abbreviation = 'IA'
database_table_name = 'us_ia_legislation'
legislator_table_name = 'us_ia_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

# Get the crawl delay specified in the website's robots.txt file
url = 'https://www.legis.iowa.gov/legislation/findLegislation/allbills?ga=89'
base_url = 'https://www.legis.iowa.gov'
crawl_delay = scraper_utils.get_crawl_delay(base_url)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome('../../../../../web_drivers/chrome_win_89.0.4389.23/chromedriver.exe', options=chrome_options)
print("driver found")

bill_type_dict = {
    'SF': 'Bill',
    'SJR': 'Resolution',
    'SCR': 'Resolution',
    'SR': 'Resolution',
    'SSB': 'Bill',
    'HF': 'Bill',
    'HJR': 'Resolution',
    'HCR': 'Resolution',
    'HSB': 'Bill',
    'HR': 'Resolution'
}


def get_bill_links(url):
    link_dict = []
    url_request = requests.get(url)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_table = url_soup.find('table', {'id': 'sortableTable'})
    for item in url_table.find('tbody').find_all('tr'):
        td_list = item.find_all('td')
        bill_link = base_url + td_list[1].find('a').get('href')
        session = re.search('ga=[0-9]+', bill_link).group().replace('ga=', '')
        bill_name = bill_link.split('ba=')[1]
        bill_type_key = ''.join(filter(lambda x: not x.isdigit(), bill_name)).strip()
        bill_type = bill_type_dict[bill_type_key]
        bill_title = td_list[2].text
        sponsors = [x.replace('\xa0', ' ').strip() for x in td_list[-1].text.split(',')]
        if len(sponsors) >= 1:
            for sponsor in sponsors:
                if ' and ' in sponsor:
                    sponsors.remove(sponsor)
                    split_sponsor = [x.title().strip() for x in sponsor.split('and')]
                    sponsors += split_sponsor
        link_dict.append({
            'bill_link': bill_link.replace(' ', '%20'),
            'bill_title': bill_title,
            'bill_name': bill_name,
            'bill_type': bill_type,
            'session': session,
            'sponsors': sponsors,
            'bill_type_key': bill_type_key
        })
    scraper_utils.crawl_delay(crawl_delay)
    return link_dict


def scrape_link(dict_item):
    row = scraper_utils.initialize_row()

    row.bill_name = dict_item['bill_name']
    row.bill_title = dict_item['bill_title']
    row.bill_type = dict_item['bill_type']
    row.session = dict_item['session']

    key = dict_item['bill_type_key']
    if key[0] == 's':
        chamber = 'Senate'
    else:
        chamber = 'House'
    row.chamber_origin = chamber

    try:
        sponsors = dict_item['sponsors']
        if len(sponsors) == 1:
            if 'Committee' in sponsors[0].title():
                row.principal_sponsor = sponsors[0].title()
                row.committees = [{
                    'chamber': chamber,
                    'committee': sponsors[0]
                }]

            else:
                principal_sponsor = sponsors[0].title()

                if ' ' in principal_sponsor:
                    first_name_initial = principal_sponsor.split()[0].replace('.', '').title().strip()

                    name_last = principal_sponsor.split()[1].replace('.', '').title().strip()
                    search_for = dict(name_last=name_last)

                    row.principal_sponsor = principal_sponsor.title()
                    row.principal_sponsor_id = scraper_utils.legislators_search_startswith(
                        'goverlytics_id', 'name_first', first_name_initial, **search_for
                    )
                else:
                    name_last = principal_sponsor
                    row.principal_sponsor = name_last
                    search_for = dict(name_last=name_last)
                    row.principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)
        else:
            row.sponsors = sponsors
            sponsor_id_lst = []
            com_lst = []
            for sponsor in sponsors:
                if 'Committee' in sponsor.title():
                    com_lst.append({
                        'chamber': chamber,
                        'committee': sponsor
                    })
                elif ' ' in sponsor:
                    first_name_initial = sponsor.split()[0].replace('.', '').title().strip()
                    name_last = sponsor.split()[1].replace('.', '').title().strip()
                    search_for = dict(name_last=name_last)

                    if len(first_name_initial)==1:
                        sponsor_id_lst.append(
                            scraper_utils.legislators_search_startswith(
                                'goverlytics_id', 'name_first', first_name_initial, **search_for
                            )
                        )
                    else:
                        sponsor_id_lst.append(
                            scraper_utils.get_legislator_id(**search_for)
                        )

                else:
                    name_last = sponsor.title().strip()
                    search_for = dict(name_last=name_last)

                    sponsor_id_lst.append(
                        scraper_utils.get_legislator_id(**search_for)
                    )
            row.sponsors_id = sponsor_id_lst
    except IndexError as e:
        print(f'Failed at {dict_item["bill_name"]}')
        print(e)

    link = dict_item['bill_link']
    row.source_url = link

    url_request = requests.get(link)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    try:
        bill_req = requests.get(base_url + url_soup.find('iframe').get('src'))
        bill_soup = BeautifulSoup(bill_req.content, 'lxml')

        span_lst = bill_soup.find_all('span', {'class': 't'})
        for span_item in span_lst:
            if span_item.text == '-':
                index_num = span_lst.index(span_item) + 1
                row.current_status = span_lst[index_num].text

        bill_text = bill_soup.text.replace('\n', ' ').strip()
        row.bill_text = bill_text
    except:
        print(f'Found no bill text for {link}')

    driver.get(link)
    time.sleep(sleep_time)
    see_all_button = driver.find_element_by_xpath('//*[@id="content"]/div[1]/div[6]/h2/a')
    driver.execute_script("arguments[0].click();", see_all_button)
    time.sleep(sleep_time)
    source_topic = driver.find_element_by_xpath('//*[@id="content"]/div[1]/div[6]/div').text
    row.source_topic = source_topic

    goverlytics_bill_name = dict_item["bill_name"].replace(' ', '_')
    row.goverlytics_id = f'{state_abbreviation}_{dict_item["session"]}_{goverlytics_bill_name}'

    scraper_utils.crawl_delay(crawl_delay)
    print(f'done row for {dict_item["bill_name"]}')
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_bill_links(url)[1200:-1]

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    with Pool(processes=10) as pool:
        data = pool.map(scrape_link, urls)

    # data = [scrape_link(link) for link in urls]

    # Once we collect the data, we'll write it to the database.
    # scraper_utils.write_data(data)

    print('Complete!')
