import sys
import os
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislatorScraperUtils
from urllib.request import urlopen as uReq
from tqdm import tqdm
from selenium.webdriver.support.ui import Select
import pandas as pd
from multiprocessing import Pool
from time import sleep
import time
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

BASE_URL = 'https://le.utah.gov/asp/roster/roster.asp'

WIKI_URL = 'https://en.wikipedia.org'
WIKI_REP_PATH = '/wiki/Utah_House_of_Representatives'
WIKI_SENATE_PATH = '/wiki/Utah_State_Senate'

scraper_utils = USStateLegislatorScraperUtils('UT', 'us_ut_legislators')
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)


@scraper_utils.Timer()
def open_driver(url):
    options = Options()
    options.headless = True
    # driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
    #                                                        'chrome_win_90.0.4430.24', 'chromedriver.exe'), options=options)
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get(url)
    driver.maximize_window()
    scraper_utils.crawl_delay(crawl_delay)
    return driver


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


@scraper_utils.Timer()
def get_urls(path):
    urls = []
    soup = make_soup(path)
    table = soup.find('table', {'class': 'UItable'}).find_all('tr')[1:]
    pbar = tqdm(table)
    for tr in pbar:
        try:
            link = 'https://le.utah.gov/asp/roster/' + tr.find('td').find('a').get('href')
            urls.append(link)
        except:
            pass
    return urls


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


def get_source_id(url, row):
    driver = open_driver(url)
    url = driver.current_url
    source_id = url.split('/')[-2]
    row.source_id = source_id
    driver.quit()


@scraper_utils.Timer()
def get_representative_name(url, row):
    driver = open_driver(url)
    full_name = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/h1').text.title()
    hn = HumanName(full_name)
    row.name_first = hn.first
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name
    # print(hn.full_name)
    driver.quit()


@scraper_utils.Timer()
def get_representative_email(url, row):
    driver = open_driver(url)
    email = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[3]/div/div[2]/div').text
    row.email = email
    # print(email)
    driver.quit()


@scraper_utils.Timer()
def get_representative_addresses(url, row):
    driver = open_driver(url)
    addresses = []
    location = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[1]/div/div[2]/h4').text
    address = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[1]/div/div[2]/div').text
    address_dict = {'address': address, 'location': location}
    addresses.append(address_dict)
    row.addresses = addresses
    # print(addresses)
    driver.quit()


@scraper_utils.Timer()
def get_representative_phone_number(url, row):
    driver = open_driver(url)
    try:
        phone_nums = []
        phone = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[2]/div/div[2]/div/div[2]/div').text
        phone = phone.replace('(', '').replace(')', '').replace(' ', '-')
        phone_dict = {'office': '', 'phone': phone}
        phone_nums.append(phone_dict)
        print(phone_nums)
        row.phone_numbers = phone_nums
        print(row.phone_numbers)
    except:
        pass
    driver.quit()


@scraper_utils.Timer()
def get_representative_party(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[2]').text
    row.party = text.split()[0]
    row.party_id = scraper_utils.get_party_id('Republican' if row.party == 'Republican' else 'Democrat')
    # print(row.party)
    driver.quit()


@scraper_utils.Timer()
def get_representative_role(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[1]').text
    row.role = text.split()[0]
    # print(row.role)
    driver.quit()


@scraper_utils.Timer()
def get_representative_district(url, row):
    driver = open_driver(url)
    text = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[1]/div/div[1]/div[1]/div/p[2]').text
    role = text.split('–')[1].split()
    row.district = " ".join(role[:2])
    # print(row.district)
    driver.quit()


@scraper_utils.Timer()
def get_wiki_info(url, row):
    soup = make_soup(url)
    table = soup.find('table', {'class': 'wikitable sortable'}).find('tbody').find_all('tr')
    for tr in table[1:]:
        try:
            name = tr.find_all('td')[1].text.split()
            wiki_first, wiki_last = name[0], name[1]
        except:
            pass
        if row.name_last == wiki_last and row.name_first[0:2].startswith(wiki_first[0]):
            try:
                path = tr.find_all('td')[1].find('a').get('href')
                wiki_info = scraper_utils.scrape_wiki_bio(WIKI_URL + path)
                row.education = wiki_info['education']
                row.occupation = wiki_info['occupation']
                row.years_active = wiki_info['years_active']
                row.most_recent_term_id = wiki_info['most_recent_term_id']
                if wiki_info['birthday'] is not None:
                    row.birthday = str(wiki_info['birthday'])
            except AttributeError:
                print(url)


def get_representative_committees(url, row):
    driver = open_driver(url)
    committees = []
    table = driver.find_element_by_xpath('//*[@id="et-boc"]/div/div/div[2]/div[1]/div[1]/div/div/ul').find_elements_by_tag_name('li')
    for li in table:
        role = ''
        committee = li.text
        committee_info = {'role': role, 'committee': committee}
        committees.append(committee_info)
    row.committees = committees
    driver.quit()


def get_senator_name(url, row):
    driver = open_driver(url)
    name = driver.find_element_by_xpath('//*[@id="gen-info"]/h1').text.title()
    hn = HumanName(name)
    row.name_first = hn.first
    row.name_last = hn.last
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.name_full = hn.full_name
    driver.quit()


def get_senator_email(url, row):
    driver = open_driver(url)
    email = driver.find_element_by_xpath('//*[@id="email-div"]/a').text
    row.email = email
    driver.quit()


def get_senator_addresses(url, row):
    driver = open_driver(url)
    addresses = []
    address = driver.find_element_by_xpath('//*[@id="address-div"]/p').text
    address_dict = {'address': address, 'location': ''}
    addresses.append(address_dict)
    row.addresses = addresses
    driver.quit()


def get_senator_phone_number(url, row):
    driver = open_driver(url)
    try:
        phone_nums = []
        phone_div = driver.find_element_by_xpath('//*[@id="phone-div"]/div').find_elements_by_tag_name('p')
        for par in phone_div:
            text = par.text.split(':')
            office = text[0]
            phone = text[1].strip()
            phone_dict = {'office': office, 'phone': phone}
            phone_nums.append(phone_dict)
        print(phone_nums)
        row.phone_numbers = phone_nums
        print(row.phone_numbers)
    except:
        pass
    # print(phone_nums)
    driver.quit()


def get_senator_role(url, row):
    driver = open_driver(url)
    role = driver.find_element_by_xpath('//*[@id="form-max-width"]/h2').text.split()[1].title()
    row.role = role
    driver.quit()

    # print(role)


def get_senator_district(url, row):
    driver = open_driver(url)
    district = driver.find_element_by_xpath('//*[@id="flex-service"]/div[1]').text.split('-')[-1].strip()
    row.district = district
    driver.quit()

    # print(district)


def get_senator_party(url, row):
    driver = open_driver(url)
    party = driver.find_element_by_xpath('//*[@id="flex-service"]/div[1]/span').text
    row.party = party
    driver.quit()


def get_senator_committees(url, row):
    driver = open_driver(url)
    committees = []
    ul = driver.find_element_by_xpath('//*[@id="links-div"]/div[1]').find_elements_by_tag_name('li')
    for li in ul:
        committee = li.text
        committee_dict = {'committee': committee, 'role': ''}
        committees.append(committee_dict)

    row.committees = committees
    print(committees)


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Utah_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Utah_State_Senate"

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
    row = scraper_utils.initialize_row()
    # soup = make_soup(url)
    row.source_url = url
    get_source_id(url, row)

    # why are senators and reps laid out complete differently...
    if url[-1] == 'H':
        get_representative_name(url, row)
        get_representative_email(url, row)
        get_representative_addresses(url, row)
        get_representative_phone_number(url, row)
        get_representative_role(url, row)
        get_representative_district(url, row)
        get_representative_party(url, row)
        get_wiki_info(WIKI_URL + WIKI_REP_PATH, row)
        get_representative_committees(url, row)
        get_wiki_url(row)
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender
    else:
        get_senator_name(url, row)
        get_senator_email(url, row)
        get_senator_addresses(url, row)
        get_senator_phone_number(url, row)
        get_senator_role(url, row)
        get_senator_district(url, row)
        get_senator_party(url, row)
        get_wiki_info(WIKI_URL + WIKI_SENATE_PATH, row)
        get_senator_committees(url, row)
        get_wiki_url(row)
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender
    return row


def main():
    """
    Map urls to scrape function and write to database..
    """

    urls = get_urls(BASE_URL)

    with Pool(processes=4) as pool:
        data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)

        # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Utah_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Utah_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    print('Scraping complete')

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    scraper_utils.write_data(data, 'us_ut_legislators')


if __name__ == '__main__':
    main()
