import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import requests
from multiprocessing import Pool

from nameparser import HumanName
import pandas as pd
import unidecode
import numpy as np

base_url = 'https://www.gov.mb.ca'

scraper_utils = CAProvTerrLegislatorScraperUtils('MB', 'ca_mb_legislators')
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def scrape_main_page(link):
    # get a list of links to members' bio pages through the main pages
    members = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    table = page_soup.find("div", {"class": {"calendar_wrap"}})
    table_items = table.findAll("tr")[1:]

    for ti in table_items:
        try:
            ti_info = ti.findAll("td")

            url = 'https://www.gov.mb.ca/legislature/members/' + \
                (ti_info[1].a["href"])
            party_abbrev = ti_info[2].text
            if party_abbrev == 'PC':
                party = 'Progressive Conservative'
            elif party_abbrev == 'NDP':
                party = 'New Democratic'
            elif party_abbrev == 'IND LIB':
                party = 'Liberal'
            party_id = scraper_utils.get_party_id(party)

            main_info = {'source_url': url, 'party': party, 'party_id': party_id}

            members.append(main_info)
        except:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return members


def collect_mla_data(link_party):
    # scrape member's bio pages
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
        riding = member_name.split('\n')[1].strip().replace('  ', ' ')
    except:
        member_riding = member.findAll("h2")
        riding = member_riding[1].text

    row.riding = riding
    name = name.replace("Hon. ", "").strip()
    if name == "Catherine Cox":
        name = "Cathy Cox"
    hn = HumanName(name)
    row.name_full = hn.full_name
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
    phone_numbers = []
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
            phone_info = {'office': address_location, 'number': phone}
            phone_numbers.append(phone_info)
            stop = 1
    addr_info = {'location': address_location,
                 'address': address.replace('\xa0', "").strip()}

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
                              'number': phone}
                phone_numbers.append(phone_info)

            except:
                pass
            stop = 1
    addr_info = {'location': address_location,
                 'address': address.replace('\xa0', "").strip()}

    addresses.append(addr_info)
    row.addresses = addresses

    row.phone_numbers = phone_numbers
    
    uClient = uReq('https://en.wikipedia.org/wiki/Legislative_Assembly_of_Manitoba')
    page_html = uClient.read()
    uClient.close()
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        name_td = tr.findAll("td")[1]
        name = name_td.text
        district = tr.findAll("td")[3].text
        if row.riding.lower() == district.strip().lower() and row.name_last in name.strip():
            row.wiki_url = 'https://en.wikipedia.org' + name_td.a['href']
            bio = get_biography_from_wiki(row.wiki_url)
            row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, bio)
            break

    scraper_utils.crawl_delay(crawl_delay)
    return row

def get_biography_from_wiki(link):
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    page_soup = soup(page_html, "html.parser")
    main_content = page_soup.find("div", {"id" : "content"}).text
    return main_content

def scrape_main_wiki(link):
    # get links to members' personal wikipedia pages
    wiki_urls = []
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    table = page_soup.find("table", {"class": "wikitable sortable"})
    table = table.findAll("tr")[1:]
    for tr in table:
        try:
            td = tr.findAll("td")[1]
            url = 'https://en.wikipedia.org' + (td.span.span.span.a["href"])

            wiki_urls.append(url)
        except:
            continue
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
    # drop columns that we'll get from wikipedia instead
    leg_df = leg_df.drop(columns=[
                         'birthday', 'education', 'occupation', 'years_active', 'most_recent_term_id'])

    wiki_link = 'https://en.wikipedia.org/wiki/Legislative_Assembly_of_Manitoba'
    wiki_bios = scrape_main_wiki(wiki_link)
    with Pool() as pool:
        wiki_data = pool.map(
            func=scraper_utils.scrape_wiki_bio, iterable=wiki_bios)
    wiki_df = pd.DataFrame(wiki_data)[
        ['occupation', 'education', 'birthday', 'wiki_url', 'name_last', 'years_active', 'most_recent_term_id']
    ]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["wiki_url", "name_last"])
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})
    big_df['most_recent_term_id'] = big_df['most_recent_term_id'].replace({np.nan: None})

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
