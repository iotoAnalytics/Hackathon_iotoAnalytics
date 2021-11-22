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
from unidecode import unidecode


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
    row = scraper_utils.initialize_row()

    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    scraper_utils.crawl_delay(crawl_delay)


    page_soup = soup(page_html, "html.parser")
    img = page_soup.findAll("img")
    name = (img[12]["alt"])
    hn = HumanName(name)
    row.name_full = hn.full_name
    # Names are not correct on the candidate images... hardcode fix here but should make more robust later
    if row.name_full == 'Carlos J Leitao':
        row.name_last = 'Leitão'
    elif row.name_full == 'Simon Jolin-Barette':
        row.name_last = "Barrette"
    else:
        row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    
    # member id is now source_id
    member_id = myurl.split("/index")[0]
    member_id = member_id.split("-")
    row.source_id = member_id[len(member_id) - 1]
    row.source_url = myurl

    personal_info = page_soup.find("div", {"class": "enteteFicheDepute"})
    personal_info = personal_info.findAll("li")
    riding = personal_info[0].text
    row.riding = riding.split("for ")[1].replace('’', '\'')
    row.party = personal_info[1].text.strip()

    row.committees = []
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

            row.committees.append(com_info)
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

            row.committees.append(com_info)
        elif " for " in co:
            co = co.split(" for ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}
            row.committees.append(com_info)

        elif " of " in co:
            co = co.split(" of ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            row.committees.append(com_info)
        elif " on the " in co:
            co = co.split(" on the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee,
                        'house': 'National Assembly'}

            row.committees.append(com_info)

    contact_link = myurl.replace("index", "coordonnees")

    uClient = uReq(contact_link)
    page_html = uClient.read()
    uClient.close()
    scraper_utils.crawl_delay(crawl_delay)


    contact_soup = soup(page_html, "html.parser")
    address_info = contact_soup.findAll("div", {"class": "blockAdresseDepute"})
    row.phone_numbers = []
    numbers = []
    row.addresses = []

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
                        row.phone_numbers.append(num_info)
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
            row.addresses.append(addr_info)

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
    row.email = email
    
    row.party = row.party.title()
    if row.party == "Quebec Liberal Party":
        row.party = "Liberal"
    elif row.party == "Parti Conservateur Du Québec":
        row.party = "Conservative"

    try:
        row.party_id = scraper_utils.get_party_id(row.party)
    except:
        row.party_id = 0

    uClient = uReq('https://en.wikipedia.org/wiki/National_Assembly_of_Quebec')
    scraper_utils.crawl_delay(crawl_delay)

    page_html = uClient.read()
    uClient.close()
    wiki_page_soup = soup(page_html, "html.parser")

    row.most_recent_term_id = get_most_recent_term_id_from_wiki(wiki_page_soup)

    table = wiki_page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        tds = tr.findAll("td")
        if len(tds) != 4:
            continue
        district = tds[3].text
        name_td = tds[1]
        name = name_td.text
        if unidecode(row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(row.name_last.lower()) in unidecode(name.strip().lower()):
            row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
            bio = get_biography_from_wiki(row.wiki_url)
            try:
                row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio)
            except:
                print(row.name_full)
            break

    scraper_utils.crawl_delay(crawl_delay)
    return row

def get_most_recent_term_id_from_wiki(page_soup):
    info_box = page_soup.find('table', {'class':'infobox vcard'})
    current_legislature = info_box.findAll('tr')[1].text
    current_legislature = current_legislature.split(' ')[0]
    return current_legislature
    
def get_biography_from_wiki(link):
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    page_soup = soup(page_html, "html.parser")
    main_content = page_soup.find("div", {"id" : "content"}).text
    return main_content

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

            biolink = "https://en.wikipedia.org" + \
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

try:
    if __name__ == '__main__':
        with Pool() as pool:
            leg_data = pool.map(func=collect_leg_data, iterable=assembly_members)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        leg_df = pd.DataFrame(leg_data)

        leg_df = leg_df.drop(columns=['birthday', 'education', 'occupation', 'years_active'])

        # get my missing info from wikipedia
        wiki_link = 'https://en.wikipedia.org/wiki/National_Assembly_of_Quebec'
        wiki_people = get_wiki_people(wiki_link)

        with Pool() as pool:
            wiki_data = pool.map(
                func=scraper_utils.scrape_wiki_bio, iterable=wiki_people)
        wiki_df = pd.DataFrame(wiki_data)

        wikidf = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation', 'years_active']]
        # print(wikidf)
        big_df = pd.merge(leg_df, wikidf, how='left',
                        on=["wiki_url"])

        big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
        big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
        big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
        big_df['education'] = big_df['education'].replace({np.nan: None})

        big_list_of_dicts = big_df.to_dict('records')
        # print(big_list_of_dicts)

        print('Writing data to database...')

        scraper_utils.write_data(big_list_of_dicts)

        print('Complete!')
except Exception as e:
    print(e)
    sys.exit(1)