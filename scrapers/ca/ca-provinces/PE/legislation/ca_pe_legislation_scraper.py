'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import boto3
from datetime import datetime
from pprint import pprint
from urllib.parse import parse_qs
import urllib.parse as urlparse
import re
from nameparser import HumanName
import configparser
from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from scraper_utils import CAProvinceTerrLegislationScraperUtils
import dateutil.parser as dparser
from selenium import webdriver
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys


PATH = "../../../../../web_drivers/chrome_win_91.0.4472.19/chromedriver.exe"
browser = webdriver.Chrome(PATH)

prov_terr_abbreviation = 'PE'
database_table_name = 'ca_pe_legislation'
legislator_table_name = 'ca_pe_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)

base_url = 'https://www.assembly.pe.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def search_current_bills(scrape_url):
    url_list = []
    browser.get(scrape_url)
    current_year = datetime.now().year
    search = browser.find_element_by_name("year")
    search.send_keys(f"{current_year}")
    time.sleep(3)
    browser.find_element_by_id("edit-submit").click()
    time.sleep(3)

    while True:
        try:
            element = browser.find_element_by_partial_link_text("Next")
            url = browser.current_url
            url_list.append(url)
            element.click()
            time.sleep(3)
        except ElementClickInterceptedException:
            break
    url = browser.current_url
    url_list.append(url)

    return url_list


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    path = '/legislative-business/house-records/bills#/service/LegislativeAssemblyBillProgress' \
           '/LegislativeAssemblyBillSearch '
    scrape_url = base_url + path
    bills_page_urls = search_current_bills(scrape_url)

    for bill_url in bills_page_urls:
        browser.get(bill_url)
        time.sleep(3)
        table = browser.find_element_by_tag_name("tbody")
        rows = table.find_elements_by_tag_name('tr')
        for row in rows:
            a = row.find_element_by_tag_name('a')
            link = a.get_attribute('href')
            urls.append(link)
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def get_bill_name(row):
    elements = browser.find_elements_by_tag_name('h2')
    for elem in elements:
        if "Bill" in elem.text:
            bill_text = elem.text
            bill_text = bill_text.split(' - ')[0]
            bill_num = re.findall(r'[0-9]', bill_text)
            num = ''.join(bill_num)
            zero_filled_number = num.zfill(3)
            bill_name = 'Bill' + zero_filled_number
            row.bill_name = bill_name
            return bill_name


def get_session(row):

    elements = browser.find_elements_by_tag_name('p')
    for elem in elements:
        if "information about Bill" in elem.text:
            text = elem.text
    try:
        text = text.split('promoted during the ')[1]
        text = text.split("General Assembly")[0]
        nums = re.findall(r'\d+', text)
        session = nums[1] + '-' + nums[0]
        row.session = session
        return session

    except Exception:
        pass


def get_bill_type(main_div, row):
    table = main_div.find('table', {'class': 'views-table'})
    bill_type = table.findAll('td')[1].text
    row.bill_type = bill_type


def get_bill_title(main_div, row):
    title = main_div.find('h1')
    title = title.text.split(' - ')[0]
    try:
        title = title.split('- Bill')[0]
    except Exception:
        pass
    title = title.replace('\n', '')
    row.bill_title = title


def get_current_status(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    status = table_row[-1].findAll('td')[0].text
    if "Law Amendments" in status:
        status = table_row[-2].findAll('td')[0].text
    row.current_status = status


def get_actions(main_div, row):
    actions = []
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    try:
        for tr in reversed(table_row):
            status = tr.findAll('td')[0].text
            date = tr.findAll('td')[1].text
            try:
                date = dparser.parse(date, fuzzy=True)
                date = date.strftime("%Y-%m-%d")
            except Exception:
                date = None
            if status:
                if date:
                    action = {'date': date, 'action_by': 'Legislative Assembly', 'description': status}
                    actions.append(action)
    except Exception:
        pass

    row.actions = actions


def get_get_date_introduced(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    introduced_text = table_row[1].findAll('td')[1].text
    date = dparser.parse(introduced_text, fuzzy=True)
    date = date.strftime("%Y-%m-%d")
    row.date_introduced = date


def get_bill_description(main_div, row):
    text = main_div.find('div', {'class': 'pane-ns-leg-bill-metadata'}).text
    text = text.split('Introduced')[0]
    text = text[text.index('An'):]
    text = text.replace('\n', ' ')
    row.bill_description = text


def get_bill_text(url, row):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    bill_text = soup.find('div', {'class': 'bill_text'}).text.strip()
    scraper_utils.crawl_delay(crawl_delay)
    row.bill_text = bill_text


def get_bill_link(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    try:
        for tr in table_row:
            text = tr.text
            if "Statute" in text:
                link = tr.find('a').get('href')
            elif "Bill as introduced" in text:
                link = tr.find('a').get('href')
        url = base_url + link
        get_bill_text(url, row)
    except Exception:
        pass


def get_sponsor_id(name_first, name_last):
    search_for = dict(name_last=name_last, name_first=name_first)
    sponsor_id = scraper_utils.get_legislator_id(**search_for)
    return sponsor_id


def get_principal_sponsor(main_div, row):
    text = main_div.find('div', {'class': 'pane-ns-leg-bill-metadata'}).text
    text = text.split('Introduced by ')[1]
    name = text.split(',')[0]
    hn = HumanName(name)
    name_full = name
    name_last = hn.last
    name_first = hn.first

    row.principal_sponsor = name_full
    row.principal_sponsor_id = get_sponsor_id(name_first, name_last)


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:

    row.source_url = url
    row.region = scraper_utils.get_region(prov_terr_abbreviation)
    row.chamber_origin = 'Legislative Assembly'
    #
    # page = scraper_utils.request(source_url)
    # soup = BeautifulSoup(page.content, 'html.parser')
    # main_div = soup.find('div', {'class': 'panel-display panel-1col clearfix'})
    #
    browser.get(url)

    bill_name = get_bill_name(row)
    session = get_session(row)

    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id
    print(goverlytics_id)

    # get_bill_type(main_div, row)
    # get_bill_title(main_div, row)
    # get_current_status(main_div, row)
    # get_actions(main_div, row)
    # get_bill_description(main_div, row)
    # get_bill_link(main_div, row)
    # get_principal_sponsor(main_div, row)
    # get_get_date_introduced(main_div, row)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')

    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    data = [scrape(url) for url in urls]
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    #scraper_utils.write_data(data)

    print('Complete!')
