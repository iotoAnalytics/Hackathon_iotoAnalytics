import sys
import os
from pathlib import Path
import requests
import time
from selenium import webdriver
from scraper_utils import ElectorsScraperUtils
from database import CursorFromConnectionFromPool
from bs4 import BeautifulSoup
import pdfplumber
import requests
import io
import re

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

PATH = "../../../web_drivers/chrome_win_91.0.4472.19/chromedriver.exe"
browser = webdriver.Chrome(PATH)


COUNTRY = 'ca'
TABLE = 'ca_electors'
MAIN_URL = 'https://www.elections.ca/'
ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'
THREADS_FOR_POOL = 12

scraper_utils = ElectorsScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)


def find_electors_links(link):
    page = scraper_utils.request(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    main = soup.find('div', {'id': 'content-main'})
    voters_links = main.find_all('a')
    for link in voters_links:
        if "Voting" in link.text:
            voter_link = link.get('href')
            voter_link = MAIN_URL + voter_link
            return voter_link


def get_urls():
    urls = []
    election_links = []

    page = scraper_utils.request(ELECTIONS_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    links_section = soup.find('div', {'id': 'content-main'})
    elections = links_section.find_all('a')
    for election in elections[2:]:
        link = election.get('href')
        link = MAIN_URL + link
        election_links.append(link)
    for link in election_links:
        link = find_electors_links(link)
        if link is not None:
            urls.append(link)

    return urls


def get_table_data(url):
    row = scraper_utils.initialize_row()
    print(url)
    browser.get(url)
    if browser.find_elements_by_tag_name('frame'):
        frames = (browser.find_elements_by_tag_name('frame'))
        browser.switch_to.frame(frames[0])
        input_tag = browser.find_element_by_tag_name("input")
        try:
            input_value = int(input_tag.get_attribute('value'))
        except:
            option_tag = browser.find_element_by_tag_name("option")
            input_value = int(option_tag.get_attribute('value'))
        url = url.replace('default.html', '')
        url = url.replace('home.html', '')
        data = get_election_with_frame(input_value, url)
    elements = browser.find_elements_by_tag_name('a')
    for e in elements:
        link = e.get_attribute('href')
        try:
            if '#3' in link:
                e.click()
            elif '#1' in link:
                e.click()
        except:
            pass
    links = browser.find_elements_by_tag_name('a')
    time.sleep(5)
    for l in links:
        try:
            if 'Number of electors and polling stations' in l.text:
                if 'for' not in l.text:
                    l.click()
                    data = get_election()
        except:
            pass
    print(data)


def get_election_with_frame(input_value, url):
    browser.get(url + f'{input_value}/table1.html')
    election_name =''
    try:
        election_name = browser.find_element_by_id('EventName')
    except:
        pass
    if election_name == '':
        election_name = browser.find_element_by_id('EventNameE')

    election_name = election_name.text
    return get_province_data_from_frame_table(election_name)


def get_election():
    try:
        election_name = browser.find_element_by_id('EventName')
        if election_name is None:
            election_name = browser.find_element_by_id('EventNameE')
        election_name = election_name.text
        return get_province_data(election_name)
    except:
        data = browser.find_elements_by_tag_name('b')
        for e in data:
            if 'election' in e.text:
                election_name = e.text
                election_name = election_name.split(':')[0]
                return get_province_data_from_alt_table(election_name)


def get_province_data_from_alt_table(election_name):
    provincial_data_list = []
    table = browser.find_elements_by_tag_name("tbody")[1]
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[2:]:
        if 'Totals' not in r.text:
            items = r.find_elements_by_tag_name('td')
            province = items[0].text.strip()
            province = province.split('/')[0]
            population = items[1].text.replace(' ', '')
            electors = items[2].text.replace(' ', '')
            provincial_data = {'election': election_name, 'province': province, 'population': population,
                               'electors': electors}
            provincial_data_list.append(provincial_data)
    #print(provincial_data_list)
    return provincial_data_list

def get_province_data_from_frame_table(election_name):
    print('getting frame data')
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows[8:]:
        if "Nunavut" in r.text:
            break
        else:
            items = r.find_elements_by_tag_name('td')
        province = items[0].text.strip()
        try:
            province = province.split('/')[0]
        except:
            pass
        population = items[1].text.replace(' ', '')
        electors = items[2].text.replace(' ', '')
        if province != '':
            provincial_data = {'election': election_name, 'province': province, 'population': population,
                           'electors': electors}
            provincial_data_list.append(provincial_data)
    #print(provincial_data_list)
    return provincial_data_list

def get_province_data(election):
    provincial_data_list = []
    table = browser.find_element_by_tag_name("tbody")
    rows = table.find_elements_by_tag_name('tr')
    for r in rows:
        try:
            province = r.find_element_by_tag_name('th').text
            if "Totals" not in province:
                items = r.find_elements_by_tag_name('td')
                provincial_data = {'election': election, 'province': province, 'population': items[0].text, 'electors': items[1].text}
                provincial_data_list.append(provincial_data)

        except Exception as e:
            print(e)
    #print(provincial_data_list)
    return provincial_data_list



if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()

    data = [get_table_data(url) for url in urls]

    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)


    #
    # scraper_utils.write_data(data)

    print('Complete!')
