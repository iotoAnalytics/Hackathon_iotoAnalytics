from legislator_scraper_utils import CAProvTerrLegislatorScraperUtils
import sys
import os
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
# from selenium import webdriver
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
from nameparser import HumanName
import pandas as pd
import unidecode
import numpy as np

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

scraper_utils = CAProvTerrLegislatorScraperUtils('MB', 'ca_mb_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.gov.mb.ca/')


def scrape_main_page(link):
    members = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    table = page_soup.find("div", {"class": {"calendar_wrap"}})
    table_items = table.findAll("tr")[1:]

    for ti in table_items:
        ti_info = ti.findAll("td")

        url = 'https://www.gov.mb.ca/legislature/members/' + \
            (ti_info[1].a["href"])
        party_abbrev = ti_info[2].text
        if party_abbrev == 'PC':
            party = 'Progressive Conservative Party of Manitoba'
        elif party_abbrev == 'NDP':
            party = 'New Democratic Party of Manitoba'
        elif party_abbrev == 'IND LIB':
            party = 'Manitoba Independent Liberals'
        party_id = scraper_utils.get_party_id(party)

        main_info = {'source_url': url, 'party': party, 'party_id': party_id}

        members.append(main_info)
    scraper_utils.crawl_delay(crawl_delay)
    return members


def collect_mla_data(link_party):
    link = link_party['source_url']
    row = scraper_utils.initialize_row()
    row.source_url = link
    row.role = "Member of the Legislative Assembly"
    link_id = link.split("info/")[1]
    link_id = link_id.split(".html")[0]
    row.source_id = link_id
    row.party = link_party['party']
    row.party_id = link_party['party_id']

    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    member = page_soup.find('div', {'class': 'members'})
    member_name = member.h2.text.strip()
    name = member_name.split('\n')[0]
    try:
        riding = member_name.split('\n')[1].strip()
    except:
        member_riding = member.findAll("h2")
        riding = member_riding[1].text

    row.riding = riding
    name = name.replace("Hon. ", "").strip()
    hn = HumanName(name)
    row.name_full = name
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    try:
        three = member.findAll("strong")
        maybe_email = three[0].text
        if '@' in maybe_email:
            email = maybe_email.strip()
        else:
            maybe_email = three[1].text
            if '@' in maybe_email:
                email = maybe_email.strip()

        row.email = email.replace("Email: ", "")
    except:
        try:
            three = member.find("h3")
            email = three.a.text
            row.email = email

        except:
            print(link)

    # get addresses, phone numbers
    phone_number = []
    addresses = []
    ps = member.findAll("p")
    # print(len(ps))
    hthrees = member.findAll("h3")
    if len(hthrees) == 3:
        hthrees = hthrees[1:]
    elif len(hthrees) == 4:
        hthrees = hthrees[2:]
    address_location = hthrees[0].text.split(":")[0].strip()
    ads = ps[0].text.split('\n')
    address = ads[0].strip()
    i = 1
    stop = 0
    while stop == 0:
        a = ads[i]

        if ':' not in a and '@' not in a:
            address = address + " " + a.strip()
            i = i + 1
        else:
            phone = a.split("Phone: ")[1].strip()
            phone = phone.replace("(", "")
            phone = phone.replace(") ", "-")
            phone_info = {'office': address_location, 'phone_number': phone}
            phone_number.append(phone_info)
            stop = 1
    addr_info = {'location': address_location,
                 'address': address.replace('\xa0', "").strip()}
    # print(addr_info)
    addresses.append(addr_info)

    address_location = hthrees[1].text.split(":")[0].strip()
    ads = ps[1].text.split('\n')
    address = ads[0].strip()
    i = 1
    stop = 0
    while stop == 0:
        a = ads[i]

        if ':' not in a and '@' not in a:
            address = address + " " + a.strip()
            i = i + 1
        else:
            try:
                phone = a.split("Phone: ")[1].strip()
                if "-" not in phone:
                    phone = phone + ads[i + 1]
                phone = "".join(phone.split())

                phone = phone.replace("(", "")
                phone = phone.replace(")", "-")
                phone_info = {'office': address_location,
                              'phone_number': phone}
                phone_number.append(phone_info)

            except:
                pass
            stop = 1
    addr_info = {'location': address_location,
                 'address': address.replace('\xa0', "").strip()}

    addresses.append(addr_info)
    row.addresses = addresses

    row.phone_number = phone_number
    scraper_utils.crawl_delay(crawl_delay)
    return row


def scrape_main_wiki(link):
    wiki_urls = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        td = tr.findAll("td")[1]
        url = 'https://en.wikipedia.org' + (td.span.span.span.a["href"])
        # print(url)
        wiki_urls.append(url)
    scraper_utils.crawl_delay(crawl_delay)
    return wiki_urls


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    members_link = 'https://www.gov.mb.ca/legislature/members/mla_list_constituency.html'

    member_bios = scrape_main_page(members_link)

    with Pool() as pool:

        data = pool.map(func=collect_mla_data, iterable=member_bios)
    leg_df = pd.DataFrame(data)

    leg_df = leg_df.drop(columns=[
                         'birthday', 'education', 'occupation', 'years_active', 'most_recent_term_id'])

    wiki_link = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Manitoba'
    wiki_bios = scrape_main_wiki(wiki_link)
    with Pool() as pool:
        wiki_data = pool.map(
            func=scraper_utils.scrape_wiki_bio, iterable=wiki_bios)
    wiki_df = pd.DataFrame(wiki_data)

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_first", "name_last"])
    print(big_df)
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})
    big_df['most_recent_term_id'] = big_df['most_recent_term_id'].replace({
                                                                          np.nan: None})

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')
