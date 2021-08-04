import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils
import pandas as pd
import bs4
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import psycopg2
from nameparser import HumanName
import requests
import datefinder
import unidecode
from multiprocessing import Pool
import datetime
import re
import numpy as np
from datetime import datetime


scraper_utils = CAProvTerrLegislatorScraperUtils('QC', 'ca_qc_legislators')
crawl_delay = scraper_utils.get_crawl_delay('http://www.assnat.qc.ca')


def getAssemblyLinks(myurl):
    infos = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")

    table = page_soup.find("table", {"id": "ListeDeputes"})
    trs = table.findAll("tr")[1:]
    for tr in trs:
        link = "http://www.assnat.qc.ca/" + tr.td.a["href"]
        infos.append(link)
    scraper_utils.crawl_delay(crawl_delay)
    return infos


def collect_leg_data(myurl):
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")
    img = page_soup.findAll("img")
    name = (img[12]["alt"])
    hn = HumanName(name)

    # member id is now source_id
    member_id = myurl.split("/index")[0]
    member_id = member_id.split("-")
    member_id = member_id[len(member_id) - 1]

    personal_info = page_soup.find("div", {"class": "enteteFicheDepute"})
    personal_info = personal_info.findAll("li")
    riding = personal_info[0].text
    riding = riding.split("for ")[1]
    party = personal_info[1].text.strip()

    committees = []
    uls = page_soup.findAll("ul")
    committee_offices = []
    for ul in uls:
        try:
            if ul.h4.text == "Current Offices":
                offices = ul.findAll("li")
                for office in offices:
                    committee_offices.append(office.text)
        except:
            pass
    for co in committee_offices:
        if " of the " in co:
            co = co.split(" of the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            committees.append(com_info)
        elif " to the " in co:
            co = co.split(" to the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()

            committee = committee.replace("Minister of", "").strip()

            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            committees.append(com_info)
        elif " for " in co:
            co = co.split(" for ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}
            committees.append(com_info)

        elif " of " in co:
            co = co.split(" of ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            committees.append(com_info)
        elif " on the " in co:
            co = co.split(" on the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            committees.append(com_info)

    contact_link = myurl.replace("index", "coordonnees")

    req = Request(contact_link,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    contact_soup = soup(webpage, "html.parser")
    address_info = contact_soup.findAll("div", {"class": "blockAdresseDepute"})
    phone_numbers = []
    numbers = []
    addresses = []

    for adin in address_info:
        try:
            office = adin.h3.text
            alist = (adin.address.text.split("\n"))
            tele = 0
            addr_list = []
            for a in alist:

                if "Telephone: " in a:
                    tele = 1
                    number = a.replace("Telephone: ", "").strip()
                    number = number.split(" ")[0]

                    if number not in numbers:
                        numbers.append(number)
                        num_info = {'office': office, 'number': number}
                        phone_numbers.append(num_info)
                elif "Fax" in a:
                    tele = 1
                elif "Toll" in a:
                    tele = 1

                if tele == 0:
                    addr_line = a.replace("\r", "")
                    addr_line = " ".join(addr_line.split())
                    if addr_line.strip() != "":
                        addr_list.append(addr_line.strip())

            address = ', '.join(addr_list)
            addr_info = {'location': office, 'address': address}
            addresses.append(addr_info)

        except:
            pass

    email = ""
    try:
        email = (address_info[2].address.a["href"]).replace("mailto:", "")

    except:
        try:
            email = (address_info[0].address.a["href"]).replace("mailto:", "")
        except:
            try:
                email = (address_info[1].address.a["href"]
                         ).replace("mailto:", "")
            except:
                pass
    capitalized_party = party.title()

    try:
        party_id = scraper_utils.get_party_id(capitalized_party)
    except:
        party_id = 0

    info = {'province_url': myurl, 'member_id': member_id, 'role': 'Member of National Assembly', 'name_full': name,
            'name_first': hn.first, 'name_last': hn.last, 'name_suffix': hn.suffix, 'name_middle': hn.middle,
            'riding': riding, 'party': party, 'party_id': party_id, 'email': email, 'committees': committees,
            'phone_numbers': phone_numbers, 'addresses': addresses, 'military_experience': ""}

    scraper_utils.crawl_delay(crawl_delay)
    return info


def get_wiki_people(repLink):
    # get links to legislators' personal wikipedia pages
    bio_lnks = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[1].findAll("tr")
    i = 0
    for person in people[1:]:
        try:
            info = person.findAll("td")

            biolink = "https://en.wikipedia.org/" + \
                (info[1].span.span.span.a["href"])

            bio_lnks.append(biolink)

        except:
            pass
        i += 1

    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


assembly_link = "http://www.assnat.qc.ca/en/deputes/index.html"
# get list of assembly members' bio pages
assembly_members = getAssemblyLinks(assembly_link)


if __name__ == '__main__':
    with Pool() as pool:
        leg_data = pool.map(func=collect_leg_data, iterable=assembly_members)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    leg_df = pd.DataFrame(leg_data)

    # get my missing info from wikipedia
    wiki_link = 'https://en.wikipedia.org/wiki/National_Assembly_of_Quebec'
    wiki_people = get_wiki_people(wiki_link)

    with Pool() as pool:
        wiki_data = pool.map(
            func=scraper_utils.scrape_wiki_bio, iterable=wiki_people)
    wiki_df = pd.DataFrame(wiki_data)

    # merge the wiki info with the rest of the data on first and last names
    mergedRepsData = pd.merge(leg_df, wiki_df, how='left', on=[
                              "name_first", "name_last"])

    # replace any NaN fields with None for database insertion (NaN conflicts with required data types
    mergedRepsData['most_recent_term_id'] = mergedRepsData['most_recent_term_id'].replace({
                                                                                          np.nan: None})
    mergedRepsData['years_active'] = mergedRepsData['years_active'].replace({
                                                                            np.nan: None})
    mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({
                                                                        np.nan: None})
    mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({
                                                                    np.nan: None})
    mergedRepsData['education'] = mergedRepsData['education'].replace({
                                                                      np.nan: None})
    big_df = mergedRepsData
    big_df['seniority'] = 0

    sample_row = scraper_utils.initialize_row()

    #

    big_df['province_territory'] = sample_row.province_territory
    big_df['province_territory_id'] = sample_row.province_territory_id
    #
    #
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id
    big_df['source_url'] = big_df['province_url']

    big_df['source_id'] = big_df['member_id']

    print(big_df)
    # convert to list of dicts for database insertion
    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
