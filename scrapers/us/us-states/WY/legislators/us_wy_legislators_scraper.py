import sys
import os
from pathlib import Path
from telnetlib import EC

import pandas
from selenium.webdriver.common.by import By

from scraper_utils import USStateLegislatorScraperUtils
import re
import numpy as np
from nameparser import HumanName
from multiprocessing import Pool
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

PATH = "../../../../../web_drivers/chrome_win_93.0.4577.15/chromedriver.exe"
browser = webdriver.Chrome(PATH)

state_abbreviation = 'WY'
database_table_name = 'us_wy_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)


base_url = 'https://www.wyoleg.gov'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():

    urls = []
    date = datetime.now()
    date = date.strftime("%Y")
    path_senate = f'/Legislators/{date}/S'
    path_house = f'/Legislators/{date}/H'

    # getting urls for senate
    scrape_url = base_url + path_senate
    browser.get(scrape_url)
    time.sleep(3)
    items = browser.find_elements_by_tag_name('tr')

    for tr in items[2:]:
        try:
            td = tr.find_elements_by_tag_name('td')[1]
            link = td.find_element_by_tag_name('a').get_attribute('href')
            urls.append(link)
        except:
            pass

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    # Collecting representatives urls
    scrape_url = base_url + path_house
    browser.get(scrape_url)
    time.sleep(3)
    items = browser.find_elements_by_tag_name('tr')

    for tr in items[2:]:
        try:
            td = tr.find_elements_by_tag_name('td')[1]
            link = td.find_element_by_tag_name('a').get_attribute('href')
            urls.append(link)
        except:
            pass

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        try:
            biolink = "https://en.wikipedia.org" + (info[1].a["href"])
            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def get_most_recent_term_id(row):
    date = datetime.now()
    year = date.strftime("%Y")
    row.most_recent_term_id = year


def get_district_name_and_role(browser, row):
    time.sleep(15)
    block = browser.find_element_by_class_name('col-md-9').text
    try:
        district = block.split('District ')[1]
        name = district.split(':')[1]
        district = district.split(':')[0]
        row.district = district
        print(district)
        if "Senator" in name:
            role = "Senator"
            name = name.replace("Senator", '')
        else:
            role = "Representative"
            name = name.replace("Representative", '')
        name = name.split('\n')[0]
        name = name.strip()
        hn = HumanName(name)
        row.name_full = name
        row.name_last = hn.last
        row.name_first = hn.first
        row.name_middle = hn.middle
        row.name_suffix = hn.suffix
        print(name)
        row.role = role
        print(role)
    except:
        pass


def get_areas_served_and_party(browser, row):
    time.sleep(15)
    block = browser.find_elements_by_class_name('partyHeading')
    party = block[1].text
    area = block[0].text
    row.party = party
    try:
        row.party_id = scraper_utils.get_party_id(party)
    except:
        pass
    areas = area.split(',')
    row.areas_served = areas


def get_contact_info(browser, row):
    phone_numbers = []
    addresses = []
    time.sleep(15)
    buttons = browser.find_elements_by_tag_name('li')
    for button in buttons:
        if "Contact Me" in button.text:
            button.click()
    rows = browser.find_elements_by_class_name("row")
    for item in rows:
        item = item.text
        if 'Phone' in item:
            phone = item.split(' - ')
            number = phone[1]
            number = number.replace('(', '')
            number = number.replace(')', '')
            number = number.replace(' ', '-')
            location = phone[0].split(':')[1].strip()
            phone_detail = {"office": location, "number": number}
            print(phone_detail)
            phone_numbers.append(phone_detail)
        if 'Address' in item:
            address = item.split(':')[1].strip()
            address = {'location': 'Mailing',
                       'address': address}
            addresses.append(address)
        if 'E-Mail' in item:
            email = item.split(':')[1].strip()
            row.email = email
    row.phone_numbers = phone_numbers
    row.addresses = addresses


def get_occupation(detail, row):
    job_list = []
    try:
        occupation = detail.split('Occupation:')[1]
        occupation = occupation.split('Civic')[0].strip()
        if "/" in occupation:
            occupation = occupation.split('/')
            job_list.extend(occupation)
        elif ',' in occupation:
            occupation = occupation.split(', ')
            job_list.extend(occupation)
        else:
            job_list.append(occupation)
    except:
        pass
    row.occupation = job_list


def get_years_of_service(detail, row):
    years = []
    years_served = []
    service = ''
    try:
        years_of_service = detail.split("Service:")[1]
        years_of_service = years_of_service.split('\n')
        for item in years_of_service:
            if 'Senate' in item:
                service = item
            elif 'House' in item:
                service = item
            try:
                if ', ' in service:
                    service = service.split(', ')[1]
                elif ': ' in service:
                    service = service.split(': ')[1]
            except:
                pass
            service = service.split('-')
            years.extend(service)
    except:
        pass
    try:
        years.remove('')
    except:
        pass
    for index, year in enumerate(years):
        if year == "Present":
            date = datetime.now()
            date = date.strftime("%Y")
            years[index] = int(date)
        else:
            years[index] = int(year)
    for index, year in enumerate(years):
        try:
            start_date = year
            end = years[index+1]
            for i in range(start_date, (end + 1)):
                years_served.append(i)
                i += 1
            index += 2
        except:
            pass

    years_served.sort()
    print(years_served)
    row.years_active = years_served


def get_bio(browser, row):
    time.sleep(5)
    bio_detail = browser.find_element_by_xpath('/html/body/div/div/div[2]/section/div/div[2]/div[3]/div/div/div[1]')
    detail = bio_detail.text
    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, detail)
    if not gender:
        gender = 'O'
    row.gender = gender
    get_occupation(detail, row)
    get_years_of_service(detail, row)


def get_committees(browser, row):
    committees_list = []
    time.sleep(5)
    bio_section = browser.find_element_by_xpath('/html/body/div/div/div[2]/section')
    buttons = bio_section.find_elements_by_tag_name('li')
    for button in buttons:
        if "Committees" in button.text:
            button.click()
            time.sleep(5)
    try:
        detail = bio_section.find_element_by_xpath('/html/body/div/div/div[2]/section/div/div[2]/div[3]/div/div/div[2]')
        detail = detail.text
        detail = detail.split('All')[1]
        detail_list = detail.split('\n')
        for i in range(0, (len(detail_list)-1)):
            item = detail_list[i].strip()
            if '-' in item:
                committee = item.split('-')[1]
                role = detail_list[i+1]
                committee_ = {"role": role, "committee": committee}
                committees_list.append(committee_)
    except:
        pass
    row.committees = committees_list


def scrape(url):
    print(url)
    row = scraper_utils.initialize_row()

    row.source_url = url
    browser.get(url)

    get_most_recent_term_id(row)
    get_district_name_and_role(browser, row)
    get_areas_served_and_party(browser, row)
    get_bio(browser, row)
    get_contact_info(browser, row)
    get_committees(browser, row)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) since the crawl delay is very large (ie: {crawl_delay} seconds). If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    print('Scraping data...')

    data = [scrape(url) for url in urls]

    # with Pool(processes=6) as pool:
    #     data = pool.map(scrape, urls)

    leg_df = pd.DataFrame(data)
    leg_df = leg_df.drop(columns="birthday")
    leg_df = leg_df.drop(columns="education")
    leg_df = leg_df.drop(columns="wiki_url")

    # getting urls from wikipedia
    wikipage_reps = "https://en.wikipedia.org/wiki/Wyoming_House_of_Representatives"
    wikipage_senate = "https://en.wikipedia.org/wiki/Wyoming_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))
    print(all_wiki_links)

    with Pool(processes=4) as pool:
        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last', 'wiki_url']]

    print(wiki_df)

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_first", "name_last"])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    print('Scraping complete')

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(data)

    print(f'Scraper ran successfully!')
