import os
import sys
import traceback

from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup as soup
from multiprocessing import Pool
from nameparser import HumanName
from scraper_utils import CAProvTerrLegislatorScraperUtils
from unidecode import unidecode
from urllib.request import urlopen as uReq

scraper_utils = CAProvTerrLegislatorScraperUtils('AB', 'ca_ab_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.assembly.ab.ca')


def scrape_members_link(link):
    # scrape main page to get a list of links to all the individual members' pages
    mem_bios = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    seat_plan = page_soup.find("div", {"class": "b_seatplan mx-auto"})
    divs = seat_plan.findAll("div")
    for div in divs:
        try:
            mem_bio = 'https://www.assembly.ab.ca' + div.a["href"]
            if mem_bio not in mem_bios:
                mem_bios.append(mem_bio)
        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return mem_bios


def collect_mla_data(link):
    # scrape a member's bio page to get available information
    page_html = None
    try_count = 5
    while try_count > 0:
        try:
            row = scraper_utils.initialize_row()
            uClient = uReq(link)
            page_html = uClient.read()
            uClient.close()
            break
        except:
            try_count -= 1
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    row.source_url = link
    member_id = link.split("mid=")[1]
    member_id = member_id.split("&")[0]
    row.source_id = member_id
    name_class = page_soup.find("h2", {"class": "nott ls1"})
    name = name_class.text

    name = name.replace("Honourable", "").strip()
    name = name.replace("Premier", "").strip()
    if "Mr." in name:
        name = name.split("Mr. ")[1]
    elif "Mrs." in name:
        name = name.split("Mrs. ")[1]
    elif "Ms." in name:
        name = name.split("Ms. ")[1]
    # print(type(name))
    name = name.split(",")[0]
    hn = HumanName(name)
    row.name_full = hn.full_name
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    row.role = "Member of the Legislative Assembly"

    party_info = page_soup.find("div", {"class": "mla_pa mla_table"})
    party_info = party_info.find("div", {"class": "col3"})
    party_info = party_info.find("span", {"class": "data"})
    party = party_info.text
    if party == "Independent":
        row.party = "Independence"
    elif party == "Alberta New Democratic Party":
        row.party = "New Democratic"
    else:
        row.party = party
    try:
        row.party_id = scraper_utils.get_party_id(row.party)
    except:
        row.party_id = 0

    const = page_soup.find("div", {"class": "col-lg-6 my-3 px-3 px-lg-0"})
    const = const.findAll("p")
    riding = const[1].text
    riding = riding.split(" for ")[1].strip()
    row.riding = riding

    # years active
    dates_of_service = page_soup.find("div", {"class": "mla_dos mla_table"})
    dates_of_service = dates_of_service.findAll("span")
    start = (dates_of_service[1].text)
    start = int(start.split("-")[0])

    end = (dates_of_service[3].text)

    if end == 'Current':
        # this will have to be updated each year, should be current year + 1
        years_active = list(range(int(start), 2022))
    else:
        end = int(start.split("-")[0])
        years_active = list(range(int(start), end))
    row.years_active = years_active
    addresses = []
    contacts = page_soup.findAll("div", {"class": "col-lg-auto pb-2"})
    for contact in contacts:
        a_contacts = contact.findAll("a")
        for a in a_contacts:
            try:
                if "@" in a["title"]:
                    row.email = (a["title"])
            except:
                pass

    card_body = page_soup.find(
        "div", {"class": "card-body bg-white mla-contact"})
    addr_location = card_body.findAll("div", {"class": "col-lg-2 pb-2"})
    addr = card_body.findAll("div", {"class": "col-lg-3 pb-2"})
    exc = 0
    i = 0
    while exc == 0:
        try:
            location = (addr_location[i].text)
            address = (addr[i].text).replace("Google Map", "")
            address = address.replace("\xa0", "").strip()
            addr_info = {'location': location, 'address': address}

            addresses.append(addr_info)
            i += 1
        except:
            exc = 1
    row.addresses = addresses
    phone_numbers = []
    phone_soup = page_soup.findAll(
        "div", {"class": "row border-bottom pt-2 ml-0 mr-0"})

    for ps in phone_soup:
        office_loc = ps.div.text
        phones = ps.find("div", {"class": "col-lg-auto pb-2"})
        phone_tags = phones.findAll()
        i = 0
        for pt in phone_tags:
            if i % 4 == 1:
                if '@' not in pt.text:
                    if office_loc != pt.text:
                        office = office_loc + " " + pt.text
                    else:
                        office = office_loc
                    try:
                        phone = (phone_tags[i + 1]["href"]
                        ).replace("tel:", "").strip()
                        phone = phone.replace(".", "-")
                        phone_info = {'office': office, 'number': phone}

                        phone_numbers.append(phone_info)
                    except:
                        pass

            i += 1
    legislator_phone_soup = page_soup.find(
        "div", {"class": "row border-bottom ml-0 mr-0"})
    leg_div = legislator_phone_soup.find("div", {"class": "col-lg-auto pb-2"})
    leg_tags = leg_div.findAll()
    office_loc = 'Legislature Office'
    i = 0
    for lt in leg_tags:
        if i % 4 == 1:
            if '@' not in lt.text:
                if office_loc != lt.text:
                    office = office_loc + " " + lt.text
                else:
                    office = office_loc
                try:
                    phone = (phone_tags[i + 1]["href"]
                    ).replace("tel:", "").strip()
                    phone = phone.replace(".", "-")
                    phone_info = {'office': office, 'number': phone}
                    # print(phone_info)
                    phone_numbers.append(phone_info)
                except:
                    pass

        i += 1
    row.phone_numbers = phone_numbers
    row.most_recent_term_id = str(years_active[len(years_active) - 1])

    committees = []
    committee_table = page_soup.findAll("div", {"class": "mla_lcm mla_table"})
    for c in committee_table[1:]:
        cname = c.find("div", {"class": "col3"})
        cname = cname.find("span", {"class": "data"}).text
        crole = c.find("div", {"class": "col4"})
        crole = crole.find("span", {"class": "data"}).text
        com_info = {'role': crole, 'committee': cname}
        committees.append(com_info)

    row.committees = committees
    scraper_utils.crawl_delay(crawl_delay)

    mla_bio = page_soup.find('div', {'id': 'mla_bio'}).text
    row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, mla_bio)

    uClient = uReq('https://en.wikipedia.org/wiki/Legislative_Assembly_of_Alberta')
    page_html = uClient.read()
    uClient.close()
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]

    for tr in table:
        tds = tr.findAll("td")
        if len(tds) != 5:
            continue
        district = tds[3].text
        name_td = tds[1]
        name = name_td.text
        if unidecode(row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(row.name_last.lower()) in unidecode(name.strip().lower()):
            row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
            break

    return row

def scrape_wiki(link):
    # get the links for all the legislator's wikipedia pages
    wiki_bios = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    rows = table.findAll("tr")[1:]

    for row in rows:
        name_td = row.findAll("td")[1]
        try:
            url_tail = name_td.find('a')["href"]
        except:
            continue
        wiki_link = 'https://en.wikipedia.org' + url_tail

        wiki_bios.append(wiki_link)
    scraper_utils.crawl_delay(crawl_delay)
    return wiki_bios

try:
    if __name__ == '__main__':
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        members_link = 'https://www.assembly.ab.ca/members/members-of-the-legislative-assembly/chamber-seating-plan'
        mla_links = scrape_members_link(members_link)

        with Pool() as pool:
            data = pool.map(func=collect_mla_data, iterable=mla_links)
        leg_df = pd.DataFrame(data)
        # get these from wikipedia instead
        leg_df = leg_df.drop(columns=['birthday', 'education', 'occupation'])

        wiki_link = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Alberta'
        wiki_people = scrape_wiki(wiki_link)
        with Pool() as pool:
            wiki_data = pool.map(
                func=scraper_utils.scrape_wiki_bio, iterable=wiki_people)
        wikidf = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation']]
        # print(wikidf)
        big_df = pd.merge(leg_df, wikidf, how='left',
                        on=["wiki_url"])

        big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
        big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
        big_df['education'] = big_df['education'].replace({np.nan: None})

        big_list_of_dicts = big_df.to_dict('records')
        # print(big_list_of_dicts)

        print('Writing data to database...')

        scraper_utils.write_data(big_list_of_dicts)

        print('Complete!')
except Exception as e:
    print(e)
    sys.exit(1)

try:
    if __name__ == '__main__':
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        members_link = 'https://www.assembly.ab.ca/members/members-of-the-legislative-assembly/chamber-seating-plan'
        mla_links = scrape_members_link(members_link)

        with Pool() as pool:
            data = pool.map(func=collect_mla_data, iterable=mla_links)
        leg_df = pd.DataFrame(data)
        # get these from wikipedia instead
        leg_df = leg_df.drop(columns=['birthday', 'education', 'occupation'])

        wiki_link = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Alberta'
        wiki_people = scrape_wiki(wiki_link)
        with Pool() as pool:
            wiki_data = pool.map(
                func=scraper_utils.scrape_wiki_bio, iterable=wiki_people)
        wikidf = pd.DataFrame(wiki_data)[
            ['birthday', 'education', 'wiki_url', 'occupation']]
        # print(wikidf)
        big_df = pd.merge(leg_df, wikidf, how='left',
                        on=["wiki_url"])

        big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
        big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
        big_df['education'] = big_df['education'].replace({np.nan: None})

        big_list_of_dicts = big_df.to_dict('records')
        # print(big_list_of_dicts)

        print('Writing data to database...')

        scraper_utils.write_data(big_list_of_dicts)

        print('Complete!')
except Exception as e:
    traceback.print_exc()
    sys.exit(1)
