'''
'''
import sys, os
from pathlib import Path
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
import re
from datetime import datetime
import boto3
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from nameparser import HumanName
import pandas as pd
import unidecode
import numpy as np

p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
from legislator_scraper_utils import CAProvTerrLegislatorScraperUtils

scraper_utils = CAProvTerrLegislatorScraperUtils('BC', 'ca_bc_legislators')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')

driver = webdriver.Chrome('../../../../web_drivers/chrome_win_89.0.4389.23/chromedriver.exe',
                          chrome_options=chrome_options)

print("driver found")

scraper_utils = CAProvTerrLegislatorScraperUtils('BC', 'ca_bc_legislators')


def get_urls(myurl):
    driver.get(myurl)
    timeout = 5

    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, 'BCLASS-Members-List'))
        WebDriverWait(driver, timeout).until(element_present)


    except:
        print("timeout")
        pass

    html = driver.page_source
    page_soup = soup(html, 'html.parser')
    print(page_soup)
    urls = []

    # print(page_soup)
    member_list = page_soup.find("ul", {"class": "BCLASS-Members-List"})
    members = member_list.findAll("li")
    for mem in members:
        try:
            mem_link = mem.a['href']
            urls.append(mem_link)
        except:
            print(mem)
    return urls


def scrape(url):
    url_broken = url.split("/")
    name_full = url_broken[len(url_broken) - 1].replace("-", ", ")
    most_recent_term_id = url_broken[len(url_broken) - 2]

    hn = HumanName(name_full)
    name_last = hn.last

    name_first = hn.first
    name_suffix = hn.suffix
    name_middle = hn.middle

    driver.get(url)
    timeout = 5

    # try:
    #     element_present = EC.presence_of_element_located((By.CLASS_NAME, 'col-xs-12 col-sm-12'))
    #     # WebDriverWait(driver, timeout).until(element_present)
    #
    #
    # except:
    #
    #     pass

    html = driver.page_source

    page_soup = soup(html, 'html.parser')

    ministertitle = page_soup.find("div", {"class": "col-xs-12 col-sm-12"})

    mlist = ministertitle.text.split('\n')

    party = ""
    years_active = []
    # riding = ""
    for item in mlist:
        try:
            if item.strip() != "":
                if "20" in item:
                    years = item.split(",")
                    for year in years:
                        if year.strip() not in years_active:
                            try:
                                year = int(year.strip())
                                years_active.append(year)
                            except:
                                pass
                elif "BC" in item:
                    party = (item.strip())
                elif "Minister" not in item and "Elected" not in item and "Speaker" not in item and \
                        "Deputy" not in item and "Council" not in item:
                    riding = (item.strip())

        except:
            pass

    years_to_add = []
    for year in years_active:
        for i in range(4):
            if (year + i) < 2022:
                years_to_add.append(year + i)
    for y in years_to_add:
        if y not in years_active:
            years_active.append(y)
    years_active.sort()

    try:
        party_id = scraper_utils.get_party_id(party)

    except:
        party_id = 0
    email_class = page_soup.findAll("div", {"class": "convertToEmail"})

    email = email_class[1].a.text

    addresses = []
    office_info = page_soup.find("div", {"class": "BCLASS-Member-Info BCLASS-Hide-For-Vacant"})
    office = " ".join(office_info.text.split())
    addr_info = {'location': office.split(":")[0].strip(), 'address': office.split(":")[1].strip()}
    addresses.append(addr_info)

    member_info = page_soup.findAll("div", {"class": "BCLASS-Constituency"})
    # for mein in member_info:
    constituency_one = member_info[0].text
    constituency_two = member_info[0].nextSibling
    constituency_two = constituency_two.nextSibling
    constituency_two = constituency_two.nextSibling

    address = " ".join([constituency_one.strip(), constituency_two.strip()])
    address = " ".join(address.split())
    addr_info = {'location': 'Constituency', 'address': address}
    addresses.append(addr_info)

    committees = []
    committee_info = page_soup.find("div", {"class": "BCLASS-member-cmts"})
    if committee_info is None:
        committee_info = page_soup.find("ul", {"class": "BCLASS-Members-Cmt-List"})
    try:
        coms = committee_info.findAll("li")
        for com in coms:
            committee_name = com.a.text

            committee = {'role': 'Member', 'committee': committee_name}
            committees.append(committee)
    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)

    phone_number = []
    contact = page_soup.find("div", {"class": "BCLASS-Member-Info BCLASS-Contact"})
    contacts = contact.text.split('\n')
    opn = contacts[2].replace("(", "").strip()
    opn = opn.replace(") ", "-")
    office_phone = {'location': 'office phone', 'number': opn}
    phone_number.append(office_phone)
    if contacts[6] != "":
        ofn = contacts[6].replace("(", "").strip()
        ofn = ofn.replace(") ", "-")
        office_fax = {'location': 'office fax', 'number': ofn}
        phone_number.append(office_fax)

    const_info = page_soup.findAll("div", {"class": "col-xs-12 col-sm-6"})
    const_info = const_info[1].text
    const_info = const_info.split("\n")
    const_info = const_info[14:]
    cp_num = (const_info[2].strip())
    if "Fax" not in cp_num:
        cp_num = cp_num.replace("(", "")
        cp_num = cp_num.replace(") ", "-")
        cp = {'location': 'constituency phone', 'number': cp_num}
        phone_number.append(cp)

    c_fax = (const_info[6]).strip()
    if c_fax != "":
        c_fax = c_fax.replace("(", "")
        c_fax = c_fax.replace(") ", "-")
        cf = {'location': 'constituency fax', 'number': c_fax}

        phone_number.append(cf)

    ctf_num = (const_info[10]).strip()
    if ctf_num != "" and "Constituency" not in ctf_num:
        ctf_num = ctf_num.replace("1 (", "1-")
        ctf_num = ctf_num.replace("(", "")
        ctf_num = ctf_num.replace(") ", "-")
        ctf = {'location': 'constituency toll free', 'number': ctf_num}
        # print(ctf)
        phone_number.append(ctf)

    #
    # print(phone_number)

    infos = {'name_full': name_full, 'name_last': name_last, 'name_first': name_first, 'name_middle': name_middle,
             'name_suffix': name_suffix, 'source_url': url, 'source_id': "", 'years_active': years_active,
             'party': party, 'party_id': party_id, 'riding': riding, 'role': 'Member of the Legislative Assembly (MLA)',
             'most_recent_term_id': most_recent_term_id, 'seniority': 0, 'email': email, 'addresses': addresses,
             'committees': committees, 'phone_number': phone_number}
    # print(infos)
    return infos


def get_wiki_people(link):
    people_links = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables_by_region = page_soup.findAll("table", {'border': "1"})
    for table in tables_by_region:
        trs = table.findAll("tr")[2:]
        for tr in trs:
            entries = tr.findAll("td")
            entries = entries[1:]
            for entry in entries:
                try:
                    link = "https://en.wikipedia.org" + entry.a["href"]
                    if "cite" not in link and "Party" not in link:
                        people_links.append(link)
                        # print(link)
                except:
                    pass

    return people_links


if __name__ == '__main__':
    members_link = 'https://www.leg.bc.ca/learn-about-us/members'
    # First we'll get the URLs we wish to scrape:
    urls = get_urls(members_link)
    less_urls = urls[:4]

    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)
    big_df = pd.DataFrame(data)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    sample_row = scraper_utils.initialize_row()
    big_df['province_territory'] = sample_row.province_territory
    big_df['province_territory_id'] = sample_row.province_territory_id

    big_df['country'] = sample_row.country

    big_df['country_id'] = sample_row.country_id

    # print(big_df)

    general_election_link = 'https://en.wikipedia.org/wiki/2020_British_Columbia_general_election'

    wiki_people = get_wiki_people(general_election_link)

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, wiki_people)

    wiki_df = pd.DataFrame(wiki_data)[['occupation', 'education', 'birthday', 'name_first', 'name_last']]
    # print(wiki_df)

    mergedRepsData = pd.merge(big_df, wiki_df, how='left', on=["name_first", "name_last"])

    mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({np.nan: None})
    mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({np.nan: None})
    mergedRepsData['education'] = mergedRepsData['education'].replace({np.nan: None})

    big_df = mergedRepsData
    big_list_of_dicts = big_df.to_dict('records')
    print(big_df)
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')
