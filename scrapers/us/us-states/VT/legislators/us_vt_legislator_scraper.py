import sys
import os
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislatorScraperUtils
from tqdm import tqdm
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from urllib.request import urlopen as uReq
import pandas as pd
from multiprocessing import Pool
from time import sleep
import time
from pprint import pprint
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

BASE_URL = 'https://legislature.vermont.gov/'
REP_PATH = 'people/all/2022/House'
SENATE_PATH = 'people/all/2022/Senate'

WIKI_URL = 'https://en.wikipedia.org'
WIKI_REP_PATH = '/wiki/Vermont_House_of_Representatives'
WIKI_SENATE_PATH = '/wiki/Vermont_Senate'

scraper_utils = USStateLegislatorScraperUtils('VT', 'us_vt_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


@scraper_utils.Timer()
def make_soup(url):
    """
    Takes senator and representative paths and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    scrape_url = url
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


def open_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(ChromeDriverManager().install())
    #driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
    #                                                       'chrome_win_90.0.4430.24', 'chromedriver.exe'), options=options)
    driver.get(BASE_URL)
    driver.maximize_window()
    return driver


def get_urls(path):
    """
    Takes base URL of gov site, combine with senate OR house paths to get individual representative page URLS.
    :param path: the rest of the URL path (senator or representative listing)
    :return: a list of representative source URLS
    """

    urls = []
    driver = open_driver()
    driver.get(BASE_URL + path)
    select = Select(driver.find_element_by_name('people_length'))
    select.select_by_visible_text('All')
    sleep(1)

    table = driver.find_element_by_id('people').find_element_by_tag_name('tbody')
    all_rows = table.find_elements_by_tag_name('tr')

    pbar = tqdm(all_rows)
    for row in pbar:
        pbar.set_description('Grabbing URLs')
        link = row.find_element_by_tag_name('a').get_attribute('href')
        urls.append(link)

    driver.quit()
    return urls


def get_role(soup, row):
    """
    Find legislator role and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    header = soup.find('h1')
    role = header.text.split(' ')[0]
    row.role = role


def get_name(soup, row):
    """
    Find legislator name and set row values for first, middle, suffix, last name.

    :param soup: soup obj
    :param row: row of legislator
    """

    header = soup.find('h1')
    info = header.text.split()
    name = " ".join(info[1:])
    hn = HumanName(name)
    row.name_first = hn.first
    if row.name_first == '':
        row.name_first = name.split()[0]
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name


def get_email(soup, row):
    """
    Find legislator email and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    ## These two legislators are the ONLY ones who have the table ordered differently..
    if row.name_last == 'Westman':
        email = summary_table.find_all('dd')[5].text
        row.email = email
    elif row.name_last == 'Mazza':
        pass
    else:
        email = summary_table.find_all('dd')[3].text
        row.email = email


def get_addresses(soup, row):
    """
    Find legislator address and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    addresses = []
    address_info = {'address': '', 'location': ''}

    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    address_text = summary_table.find_all('dd')
    for address in address_text:
        if len(address.text.split(',')) == 3:
            address_info['address'] = address.text
            break

    location = summary_table.find_all('dt')[5].text
    address_info['location'] = location
    addresses.append(address_info)
    row.addresses = addresses


def get_phone_numbers(soup, row):
    """
    Find legislator phone number[s] and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    phone_numbers = []
    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    phone_number = summary_table.find_all('dd')[4].text
    phone_number = phone_number.replace("(", '').replace(")", '').replace(" ", '-')
    if phone_number == "--------":
        pass
    elif any(c.isalpha() for c in phone_number):
        pass
    else:
        phone_info = {'office': '', 'number': phone_number}
        phone_numbers.append(phone_info)
        print(phone_numbers)
        row.phone_numbers = phone_numbers


def get_committees(soup, row):
    """
    Find legislator committee info and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    all_committee_info = []
    item_list = soup.find('ul', {'class': 'item-list'})
    committees = item_list.find_all('li')
    for li in committees:
        try:
            committee = li.find('a').text.strip()
            full_info = li.text.strip('\n').strip('\t').strip()
            role = full_info.replace(committee, '').strip(', ')
            committee_info = {'role': role, 'committee': committee}
            all_committee_info.append(committee_info)
        except AttributeError:
            committee = li.text.strip()
            committee_info = {'role': '', 'committee': committee}
            all_committee_info.append(committee_info)

    row.committees = all_committee_info


def get_district(soup, row):
    """
    Find legislator district info and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    district = summary_table.find_all('dd')[0].text.strip('\n')
    row.district = district


def get_party(soup, row):
    """
    Find legislator district info and set row value.

    :param soup: soup obj
    :param row: row of legislator
    """

    summary_table = soup.find('dl', {'class': 'summary-table profile-summary'})
    party = summary_table.find_all('dd')[1].text.split('/')[0]

    row.party_id = scraper_utils.get_party_id('Republican' if party == 'Republican' else 'Democrat')
    row.party = party


def get_source_id(url, row):
    """
    Find source id info and set row value.

    :param url: url of legislator
    :param row: row of legislator
    """

    source_id = url.split('/')[-1]
    row.source_id = source_id


def get_wiki_info(row):
    """
    Grab auxillary(birthday, education, etc) info from wikipedia.
    :param row: legislator row
    """

    if row.role == 'Representative':
        url = WIKI_URL + WIKI_REP_PATH
        soup = make_soup(url)
        table = soup.find_all('table', {'class': 'wikitable sortable'})[1].find('tbody').find_all('tr')
    else:
        url = WIKI_URL + WIKI_SENATE_PATH
        soup = make_soup(url)
        table = soup.find('table', {'class': 'wikitable sortable'}).find('tbody').find_all('tr')

    for tr in table[1:]:
        name = tr.find('td').text.split()
        wiki_first, wiki_last = name[0], name[1]
        if row.name_last == wiki_last and row.name_first[0].startswith(wiki_first[0]):
            try:
                link = tr.find('td').find('a').get('href')
                wiki_info = scraper_utils.scrape_wiki_bio(WIKI_URL + link)
                row.education = wiki_info['education']
                row.occupation = wiki_info['occupation']
                row.years_active = wiki_info['years_active']
                if wiki_info['birthday'] is not None:
                    row.birthday = str(wiki_info['birthday'])
            except AttributeError:
                pass


def get_most_recent_term(soup, row):
    session = soup.find('h3', {'class': 'session-current'}).text.split()
    row.most_recent_term_id = session[0]


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Vermont_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Vermont_State_Senate"

    if row.role == "Representative":
        try:
            uClient = uReq(wikipage_reps)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()
                party = party.replace('\n', '')
                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip() and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                        pass
                if not row.wiki_url:
                    for person in rows[1:]:
                        tds = person.findAll("td")
                        name_td = tds[1]
                        name = name_td.text
                        name = name.replace('\n', '')
                        party = tds[2].text
                        party = party.strip()

                        if party == "Democratic":
                            party = "Democrat"

                        if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                            row.wiki_url = name_td.a['href']
                            break
                        elif row.party == party and row.name_last in name.strip().split()[-1]:
                            row.wiki_url = name_td.a['href']
                            break
        except Exception as e:
            print(e)
    if row.role == "Senator":

        try:
            uClient = uReq(wikipage_senate)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()

                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
            if not row.wiki_url:
                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()

                    if party == "Democratic":
                        party = "Democrat"

                    if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
        except Exception as e:
            print(e)
            pass


def scrape(url):
    """
    Initialize row and scrape legislator data, setting info to the row.

    :param url: legislator url
    """

    soup = make_soup(url)
    row = scraper_utils.initialize_row()
    row.source_url = url
    get_role(soup, row)
    get_name(soup, row)
    get_email(soup, row)
    get_addresses(soup, row)
    get_committees(soup, row)
    get_district(soup, row)
    get_party(soup, row)
    get_source_id(url, row)
    get_wiki_info(row)
    get_most_recent_term(soup, row)
    get_phone_numbers(soup, row)
    get_wiki_url(row)

    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender

    return row


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("table")
    rows = tables[3].findAll("tr")

    for person in rows[1:]:
        info = person.findAll("td")
        try:
            biolink = info[1].a["href"]

            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def main():
    """
    Map urls to scrape function and write to database..
    """

    representatives_urls = get_urls(REP_PATH)
    senate_urls = get_urls(SENATE_PATH)
    urls = representatives_urls + senate_urls

    with Pool() as pool:
        data = pool.map(scrape, urls)

    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Vermont_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Vermont_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['wiki_url'] = big_df['wiki_url'].replace({np.nan: None})

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    scraper_utils.write_data(big_list_of_dicts)


if __name__ == '__main__':
    main()
