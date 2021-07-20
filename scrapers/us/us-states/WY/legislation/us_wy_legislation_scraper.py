import sys
import os
from pathlib import Path
import pdfplumber
import io
from datetime import datetime
import re
from nameparser import HumanName
import requests
from scraper_utils import USStateLegislationScraperUtils
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

state_abbreviation = 'WY'
database_table_name = 'us_wy_legislation'
legislator_table_name = 'us_wy_legislators'
scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www.wyoleg.gov/'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():

    urls = []
    current_year = datetime.now().year
    path = f"Legislation/{current_year}"
    scrape_url = base_url + path
    browser.get(scrape_url)
    time.sleep(5)
    table = browser.find_element_by_xpath("/html/body/div/div/div[2]/section/div/div[5]/table/tbody[2]")
    rows = table.find_elements_by_tag_name("tr")

    for r in rows:
        columns = r.find_elements_by_tag_name('td')
        link = columns[0].find_element_by_tag_name('a').get_attribute('href')
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
    title = browser.find_element_by_tag_name('h3').text
    title = title.split('-')[1]
    title = title.strip()
    print(title)
    row.bill_title = title


def get_current_status(row):
    time.sleep(10)
    sections = browser.find_elements_by_class_name('col-md-6')
    for section in sections:
        section = section.text
        if "Action:" in section:
            action = section.split("Action:")[1].strip()
            row.current_status = action


def get_actions(row):
    actions = []
    status = browser.find_element_by_xpath('//*[@id="tabsetTabs"]/li[2]/a')
    status.click()
    table = browser.find_element_by_xpath('/html/body/div/div/div[2]/section/div/div[2]/div/div/div[2]/div/div/div/table/tbody')
    table_row = table.find_elements_by_tag_name('tr')
    for tr in reversed(table_row[1:]):
        date = tr.find_elements_by_tag_name('td')[0].text
        status = tr.find_elements_by_tag_name('td')[1].text
        by = tr.find_elements_by_tag_name('td')[2].text
        try:
            date = dparser.parse(date, fuzzy=True)
            date = date.strftime("%Y-%m-%d")
        except Exception:
            pass
        action = {'date': date, 'action_by': by, 'description': status}
        actions.append(action)
    get_get_date_introduced(row, actions)
    row.actions = actions


def get_get_date_introduced(row, actions):
    action = actions[1]
    date = action['date']
    print(date)
    row.date_introduced = date


def get_bill_text(row):
    url = get_bill_link()
    text = ""
    try:
        response = requests.get(url, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        for page in pages:
            try:
                page_text = page.extract_text()
                text += page_text.strip()
            except Exception:
                pass
        text = text.replace('\n', '')
        row.bill_text = text
    except Exception:
        row.bill_text = text


def get_bill_link():
    section = browser.find_element_by_xpath('/html/body/div/div/div[2]/section/div/div[2]/div/div/div[1]/div[2]/div[2]/div')
    section_text = section.text
    links = section.find_elements_by_tag_name('div')
    if "Enrolled" in section_text:
        for div in links:
            if "Enrolled" in div.text:
                url = div.find_element_by_tag_name('a').get_attribute('href')
    else:
        for div in links:
            if "Introduced" in div.text:
                url = div.find_element_by_tag_name('a').get_attribute('href')

    return url


def get_sponsor_id(name_last):
    search_for = dict(name_last=name_last)
    sponsor_id = scraper_utils.get_legislator_id(**search_for)
    return sponsor_id


def get_sponsors(row):
    cosponsors = []
    sections = browser.find_elements_by_class_name('col-md-6')
    for section in sections:
        section = section.text
        if "Co-Sponsor" in section:
            try:
                sponsor = section.split(":")[1]
                sponsor = sponsor.replace('\nRepresentative(s) ', '')
                sponsor = sponsor.replace('\nSenator(s) ', ',')
                if ", " in sponsor:
                    cosponsors.extend(sponsor.split(','))
                else:
                    cosponsors.append(sponsor)
                row.cosponsors = cosponsors
            except:
                pass
        elif "Sponsor" in section:
            try:
                sponsor = section.split(":")[1].strip()
                if "Representative" in sponsor:
                    name = sponsor.split("Representative")[1].strip()
                    row.principal_sponsor = name
                elif "Senator" in sponsor:
                    name = sponsor.split("Senator")[1].strip()
                    row.principal_sponsor = name
                else:
                    committee = sponsor
                    row.principal_sponsor = committee
            except:
                pass
        try:
            row.principal_sponsor = get_sponsor_id(name)
        except:
            pass
    ids = []
    for person in cosponsors:
        if person != '':
            person = person.strip()
            s_id = get_sponsor_id(person)
            ids.append(s_id)
    row.cosponsors_id = ids


def get_summary(row):
    try:
        summary = browser.find_element_by_xpath('//*[@id="tabsetTabs"]/li[7]/a')
        summary.click()
        text = browser.find_element_by_xpath('/html/body/div/div/div[2]/section/div/div[2]/div/div/div[7]/div/div').text
        text = text.split('Elements:')[1]
        row.bill_summary = text
    except:
        pass


def get_votes(row):
    try:
        votes = browser.find_element_by_xpath('//*[@id="tabsetTabs"]/li[5]/a')
        votes.click()
        time.sleep(10)
        vote_text = browser.find_elements_by_class_name("panel-body")
        for item in vote_text:
            print(item.text)
    except:
        pass


def scrape(url):

    row = scraper_utils.initialize_row()

    row.source_url = url
    bill_name = url.split('/')[5]
    print(bill_name)
    if "H" in bill_name:
        row.chamber_origin = 'House'
    if "S" in bill_name:
        row.chamber_origin = 'Senate'
    session = datetime.now().year

    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id

    browser.get(url)
    time.sleep(10)

    #get_bill_type(row)
    #get_bill_title(row)
   # get_current_status(row)
   # get_sponsors(row)
    #get_bill_text(row)
    #get_summary(row)
    #get_actions(row)
    get_votes(row)

    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()

    data = [scrape(url) for url in urls]


    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    # data = clear_none_value_rows(data)
    # big_list_of_dicts = clear_none_value_rows(data)
    #
    # scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
