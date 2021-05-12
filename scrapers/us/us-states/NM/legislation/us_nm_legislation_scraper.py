from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys
import os
from pathlib import Path
from time import sleep
from tqdm import tqdm
import pandas as pd
from datetime import datetime

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

state_abbreviation = 'NM'
database_table_name = 'us_nm_legislation'
legislator_table_name = 'us_nm_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www.nmlegis.gov/Legislation/Legislation_List'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def open_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(
        executable_path='H:\Projects\IOTO\goverlytics-scrapers\web_drivers\chrome_win_90.0.4430.24\chromedriver.exe',
        options=options)
    driver.get(base_url)
    driver.maximize_window()
    return driver


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    return soup


def get_urls():
    """
    Grab URLS from legislation list.

    :return: a list of URLs
    """

    urls = []
    driver = open_driver()
    button = driver.find_element_by_id('MainContent_btnSearch')
    button.click()
    table = driver.find_element_by_css_selector('#MainContent_gridViewLegislation > tbody').find_elements_by_tag_name(
        'tr')
    sleep(2)

    pbar = tqdm(table[0:50])
    for row in pbar:
        link = row.find_element_by_tag_name('a').get_attribute('href')
        pbar.set_description(f'Scraping {link}')
        urls.append(link)
        scraper_utils.crawl_delay(crawl_delay)

    driver.quit()
    return urls


def open_bill_link(url):
    """
    Opens the link to the bill url.

    :param url: URL to bill
    :return: new selenium driver object
    """
    driver = open_driver()
    bill = driver.get(url)
    driver.maximize_window()
    driver.quit()
    return bill


def get_bill_name(url, row):
    """
    Grab bill name and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    name = soup.find('span', {'id': 'MainContent_formViewLegislation_lblTitle'}).text
    row.bill_name = name


def get_bill_title(url, row):
    """
    Grab bill title and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    title = soup.find('span', {'id': 'MainContent_formViewLegislation_lblTitle'}).text
    row.bill_title = title


def get_bill_sponsor_info(url, row):
    """
    Grab bill sponsor information(names, ID)

    :param url: Bill URL
    :param row: Bill row
    """

    sponsors = []
    sponsor_ids = []
    soup = make_soup(url)
    table = soup.find('table', {'id': 'MainContent_formViewLegislation'}).find_all('a')
    for name in range(len(table) - 1):
        name = table[name].text
        hn = HumanName(name)
        sponsors.append(f'{hn.last}, {hn.first}')
        sponsor_id = scraper_utils.get_legislator_id(name_last=hn.last, name_first=hn.first)
        sponsor_ids.append(sponsor_id)

    row.sponsors_id = sponsor_ids
    row.sponsors = sponsors


def get_session(url, row):
    """
    Grab bill session and set it to row.

    :param url: Bill URL
    :param row: Bill row
    """

    soup = make_soup(url)
    header = soup.find('span', {'id': 'MainContent_formViewLegislationTitle_lblSession'}).text
    session = header.split('-')[0]
    row.session = session


def translate_abbreviations():
    """
    Takes abbreviation table (for bill actions) and translates it into its corresponding description.
    :return: a dictionary with abbreviations as keys and descriptions as values
    """

    translation_dict = {}
    soup = make_soup('https://www.nmlegis.gov/Legislation/Action_Abbreviations')
    table = soup.find('table', {'id': 'MainContent_gridViewAbbreviations'}).find_all('tr')
    for tr in table:
        columns = tr.find_all('td')
        for _ in columns:
            translation_dict[columns[0].text.strip()] = columns[1].text.strip()
    return translation_dict


def get_bill_actions(url, row):
    """

    :param url:
    :param row:
    :return:
    """
    bill_actions = []

    driver = open_driver()
    driver.get(url)
    button = driver.find_element_by_css_selector('#MainContent_tabContainerLegislation_tabPanelActions_tab')
    button.click()
    table = driver.find_element_by_id('MainContent_tabContainerLegislation_tabPanelActions_dataListActions')
    sleep(1)

    actions = table.find_elements_by_tag_name('span')

    abbreviation_dict = translate_abbreviations()
    for action in actions:
        action_dict = {'date': '', 'action_by': '', 'description': ''}
        info = action.text.split('\n')
        single_line = info[0].split('-')
        if len(info) == 3:
            for word in info[2].split():
                clean_word = re.sub('\\W+', '', word)
                if clean_word in abbreviation_dict.keys():
                    info[2] = info[2].replace(word, abbreviation_dict[clean_word].lower().title())

            action_dict['description'] = info[2]
            action_dict['date'] = str(datetime.strptime(info[1].split(':')[1].strip(), '%m/%d/%Y').strftime('%Y-%m-%d'))

        elif len(single_line) == 3:
            soup = make_soup(url)
            header = soup.find('span', {'id': 'MainContent_formViewLegislationTitle_lblSession'}).text
            session = header.split('-')[0].split()[0]

            date_without_year = single_line[2].replace('.', '').strip() + f' {session}'
            converted_date_format = str(datetime.strptime(date_without_year, '%b %d %Y').date())

            action_dict['date'] = converted_date_format
            action_dict['description'] = single_line[0].strip()

        elif len(single_line) <= 2:
            new_info = info[0].split()
            for word in new_info:
                clean_word = re.sub('\\W+', '', word)
                if clean_word in abbreviation_dict.keys():
                    info[0] = info[0].replace(word, abbreviation_dict[clean_word])

            action_dict['description'] = info[0]

        bill_actions.append(action_dict)

    pprint(bill_actions)
    driver.quit()
    # return bill_actions


def scrape(url):
    row = scraper_utils.initialize_row()
    row.source_url = url

    # bill_link = open_bill_link(url)
    # get_bill_title(url, row)
    # get_bill_name(url, row)
    # get_bill_sponsor_info(url, row)
    # get_session(url, row)
    get_bill_actions(url, row)
    # translate_abbreviations()


def main():
    urls = get_urls()
    # pprint(urls)

    with Pool() as pool:
        data = pool.map(scrape, urls)

    # lst = ['https://www.nmlegis.gov/Legislation/Legislation?chamber=H&legType=B&legNo=1&year=21',
    #        'https://www.nmlegis.gov/Legislation/Legislation?chamber=H&legType=B&legNo=3&year=21',
    #        'https://www.nmlegis.gov/Legislation/Legislation?chamber=H&legType=B&legNo=7&year=21']
    # for url in lst:
    #     row = scraper_utils.initialize_row()
    #     get_bill_actions(url, row)


if __name__ == '__main__':
    main()
