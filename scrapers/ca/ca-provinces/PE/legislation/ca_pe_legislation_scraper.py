
import sys
import os
from pathlib import Path
import pdfplumber
import io
from datetime import datetime
import re
from nameparser import HumanName
import requests
from scraper_utils import CAProvinceTerrLegislationScraperUtils
import dateutil.parser as dparser
from selenium import webdriver
import time
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import pandas as pd
from multiprocessing import Pool

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

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

    urls = []

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
    time.sleep(3)
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


def get_bill_type(row):
    row.bill_type = "Bill"


def get_bill_title(row):
    elements = browser.find_elements_by_tag_name('h2')
    for elem in elements:
        if "Bill" in elem.text:
            bill_text = elem.text
            title = bill_text.split(' - ')[1]
            row.bill_title = title


def get_current_status(row, actions):
    current_status_details = actions[0]
    row.current_status = current_status_details['description']


def get_actions(row):
    actions = []
    table_row = browser.find_elements_by_tag_name('tr')
    try:
        for tr in reversed(table_row):
            status = tr.find_elements_by_tag_name('td')[0].text
            date = tr.find_elements_by_tag_name('td')[1].text
            try:
                date = dparser.parse(date, fuzzy=True)
                date = date.strftime("%Y-%m-%d")
            except Exception:
                date = None
            if "PDF" not in status:
                if date:
                    action = {'date': date, 'action_by': 'Legislative Assembly', 'description': status}
                    actions.append(action)
    except Exception:
        pass
    get_current_status(row, actions)
    get_get_date_introduced(row, actions)
    row.actions = actions


def get_get_date_introduced(row, actions):
    for action in actions:
        if action['description'] == "First Reading":
            date = action['date']
            row.date_introduced = date


def get_bill_text(url, row):
    try:
        response = requests.get(url, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        text = ""
        for page in pages:
            page_text = page.extract_text()
            text += page_text.strip()
        text = text.replace('\n', '')
        row.bill_text = text
    except Exception:
        row.bill_text = ""


def get_bill_link(row):
    table_row = browser.find_elements_by_tag_name('tr')
    try:
        for line in table_row:
            status = line.find_elements_by_tag_name('td')[0].text
            if "Bill Text" in status:
                pdf_link = line.find_element_by_tag_name('a')
                link = pdf_link.get_attribute('href')
                get_bill_text(link, row)
    except Exception:
        pass


def get_sponsor_id(name_first, name_last):
    search_for = dict(name_last=name_last, name_first=name_first)
    sponsor_id = scraper_utils.get_legislator_id(**search_for)
    return sponsor_id


def get_principal_sponsor(row):
    table_row = browser.find_elements_by_tag_name('tr')
    for line in table_row:
        status = line.find_elements_by_tag_name('td')[0].text
        if "Promoted by" in status:
            name = line.find_elements_by_tag_name('td')[1].text
            hn = HumanName(name)
            name_full = name
            name_last = hn.last
            name_first = hn.first
            row.principal_sponsor = name_full
            row.principal_sponsor_id = get_sponsor_id(name_first, name_last)


def clear_none_value_rows(data):
    df = pd.DataFrame(data)
    list_of_dicts = df.to_dict('records')
    print(list_of_dicts)
    for i in range(len(list_of_dicts)):
        value = list_of_dicts[i]['goverlytics_id']
        print(value)
        if 'None' in value:
            del list_of_dicts[i]
    return list_of_dicts


def scrape(url):

    row = scraper_utils.initialize_row()

    row.source_url = url
    row.region = scraper_utils.get_region(prov_terr_abbreviation)
    row.chamber_origin = 'Legislative Assembly'

    browser.get(url)

    bill_name = get_bill_name(row)
    session = get_session(row)

    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id

    get_bill_type(row)
    get_bill_title(row)
    get_actions(row)
    get_bill_link(row)
    get_principal_sponsor(row)

    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    #data = [scrape(url) for url in urls]
    with Pool(processes=4) as pool:
        data = pool.map(scrape, urls)
    list_of_dicts = clear_none_value_rows(data)
    print(list_of_dicts)


    #scraper_utils.write_data(data)

    print('Complete!')
