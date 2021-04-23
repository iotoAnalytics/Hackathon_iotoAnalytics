import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from request_url import UrlRequest
from legislation_scraper_utils import USStateLegislationScraperUtils
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

state_abbreviation = 'DE'
database_table_name = 'us_de_legislation'
legislator_table_name = 'us_de_legislators'
scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
url = 'https://legis.delaware.gov/AllLegislation'
base_url = 'https://legis.delaware.gov'
sleep_time = 1.5

crawl_delay = scraper_utils.get_crawl_delay(base_url)


# PATH = "C:\Program Files (x86)\chromedriver.exe"
# driver = webdriver.Chrome(PATH)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome('../../../../web_drivers/chrome_win_89.0.4389.23/chromedriver.exe', options=chrome_options)
driver.get(url)

link_lst = []

date_dict = {
    '1': '01',
    '2': '02',
    '3': '03',
    '4': '04',
    '5': '05',
    '6': '06',
    '7': '07',
    '8': '08',
    '9': '09',
    '10': '10',
    '11': '11',
    "12": '12'
}


def get_html(url):
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_summary = url_soup.find('div', {'class': 'content col-xs-24 col-sm-18 col-sm-push-6 col-sm-height col-top'})
    scraper_utils.crawl_delay(crawl_delay)
    return url_summary


def get_bill_type(bill_name):
    if 'Amendment' in bill_name:
        return 'Amendment'
    elif 'Resolution' in bill_name:
        return 'Resolution'
    elif 'Bill' in bill_name:
        return 'Bill'


def make_legis_dict(el):
    if ' ' in el.text:
        return {'name': el.text.split(' ')[1], 'id': re.search('\d+', el.get('href')).group()}
    else:
        return {'name': el.text, 'id': re.search('\d+', el.get('href')).group()}


def get_text(url):
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    url_divs = url_soup.find_all('div')
    scraper_utils.crawl_delay(crawl_delay)
    return ''.join(url_divs[2].text.split('\n'))


def get_actions(string):
    action_lst = string.split('\n')
    actions = []
    for item in action_lst:
        try:
            date = re.search('\d+/\d+/\d+', item).group()
            description = item.replace(date, '').strip()
            date = date.split('/')
            date = '20' + date[2] + '-'+date_dict[date[0]]+'-'+date[1]
            dictionary = {'date': date, 'description':description}
            if 'House' in description or 'house' in description:
                dictionary['action_by'] = 'House'
            elif 'Senate' in description or 'senate' in description:
                dictionary['action_by'] = 'Senate'
            else:
                dictionary['action_by'] = ''
            actions.append(dictionary)
        except Exception:
            pass
    return actions


def get_committees(string):
    com_string = string.split('\n')
    com_lst = []
    for item in com_string:
        try:
            el = item.replace('view', '').strip()
            com = ''.join([i for i in el if not i.isdigit()])
            com_lst.append({'chamber':'', 'committee':com.replace('//', '').strip()})
        except Exception:
            pass
    return com_lst


def search_div_lst(div_lst):
    current_status = ''
    p_sponsor = {}
    sponsors = []
    cosponsors = []
    site_topic = ''
    bill_description = ''
    bill_text = ''
    for item in div_lst:
        if item.find('h3', {'class': 'section-head'}):
            if item.find('h3', {'class': 'section-head'}).text == 'Bill Progress':
                current_status = item.find('div', {'class': 'info-value'}).text.strip()
            elif item.find('h3', {'class': 'section-head'}).text == 'Bill Details':
                for el in item.find_all('div', {'class': 'info-group'}):
                    if 'Primary Sponsor' in el.text:
                        l_name = el.text.replace('Primary Sponsor:', '').strip()
                        if len(l_name.split(' ')) == 2:
                            l_name = l_name.split(' ')[1]
                        p_sponsor['name'] = l_name
                        id = el.find('a').get('href')
                        p_sponsor['id'] = re.search('\d+', id).group()
                    elif 'Additional Sponsor(s)' in el.text:
                        for link in el.find_all('a'):
                            sponsors.append(make_legis_dict(link))
                    elif 'Co-Sponsor(s)' in el.text:
                        for link in el.find_all('a'):
                            cosponsors.append(make_legis_dict(link))
                    elif 'Long Title' in el.text:
                        site_topic = el.text.replace('Long Title:', '').strip().lower().title()
                    elif 'Original Synopsis' in el.text:
                        bill_description = el.text.replace('Original Synopsis:', '').replace('\n', '').strip()
            elif item.find('h3', {'class': 'section-head'}).text == 'Bill Text':
                for el in item.find_all('a'):
                    if 'View HTML' in el.text:
                        bill_text_url = base_url + el.get('href')
                        bill_text = get_text(bill_text_url)

    return [current_status, p_sponsor, sponsors, cosponsors, site_topic, bill_text, bill_description]


def get_id_lst(lst):
    sponsors_id = []
    for item in lst:
        try:
            sponsors_id.append(scraper_utils.get_legislator_id(name_last=item['name'], source_id=item['id']))
        except Exception:
            pass
    return sponsors_id


def make_goverlytics_id(state, session, name):
    bill_name = '_'.join(name.split(' '))
    return f'{state}_{session}_{bill_name}'


def scrape(url):
    url_summary = get_html(url)
    div_lst = get_html(url).find_all('div', {'class': 'col-xs-24'})
    div_info = search_div_lst(div_lst)
    p_sponsor = div_info[1]
    p_sponsor_id = scraper_utils.get_legislator_id(name_last=p_sponsor['name'], source_id=p_sponsor['id'])
    sponsors = [s['name'] for s in div_info[2]]
    sponsors_id = get_id_lst(div_info[2])
    cosponsors = [c['name'] for c in div_info[3]]
    cosponsors_id = get_id_lst(div_info[3])

    source_url = url
    bill_name = url_summary.find('h2').text
    session = re.search('\d+[a-z]+', url_summary.find('h5').text).group()
    bill_type = get_bill_type(bill_name)

    driver.get(url)
    # actions history
    time.sleep(sleep_time)

    goverlytics_id = make_goverlytics_id(state_abbreviation, session, bill_name)

    # commented out for now to test
    row = scraper_utils.initialize_row()
    try:
        actions = get_actions(driver.find_element_by_xpath('//*[@id="RecentReports"]/table/tbody').text)
        committees = get_committees(driver.find_element_by_xpath('//*[@id="CommitteeReportsGrid"]/table/tbody').text)
        row.committees = committees
        row.actions = actions
    except Exception:
        pass
    row.source_url = source_url
    row.session = session
    row.bill_name = bill_name
    row.bill_type = bill_type
    row.current_status = div_info[0]
    row.principal_sponsor = p_sponsor['name']
    row.sponsors = sponsors
    row.cosponsors = cosponsors
    row.principal_sponsor_id = p_sponsor_id
    row.sponsors_id = sponsors_id
    row.cosponsors_id = cosponsors_id
    row.source_topic = div_info[4]
    row.bill_text = div_info[5]
    row.bill_summary = div_info[6]
    row.goverlytics_id = goverlytics_id

    print(row.bill_name)

    return row


def add_to_link_lst():
    bill_links = []
    time.sleep(sleep_time)
    elems = driver.find_elements_by_xpath("//a[@href]")

    for elem in elems:
        if "LegislationId" in elem.get_attribute("href"):
            bill_links.append(elem.get_attribute("href"))

    if bill_links not in link_lst:
        link_lst.append(bill_links)


def click_elem():
    add_to_link_lst()
    last_page = driver.find_element_by_xpath("//a[@tabindex='-1' and @title='Go to the last page']")
    next_page = driver.find_element_by_xpath("//a[@tabindex='-1' and @title='Go to the next page']")

    print('got to page ' + str(next_page.get_attribute('data-page')))
    driver.execute_script("arguments[0].click();", next_page)
    time.sleep(sleep_time)

    if int(last_page.get_attribute('data-page')) > int(next_page.get_attribute('data-page')):
        click_elem()
    elif int(last_page.get_attribute('data-page')) == int(next_page.get_attribute('data-page')):
        add_to_link_lst()



#actual start of script:
if __name__ == '__main__':
    click_elem()
    # add_to_link_lst()
    scrape_lst = []
    for elem in link_lst:
        scrape_lst += elem
    print('Done getting list of links!')
    with Pool() as pool:
        data = pool.map(scrape, scrape_lst)
    scraper_utils.insert_legislation_data_into_db(data)

    print('Done Scraping!')

