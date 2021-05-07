import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
import pickle
import numpy as np
import gzip
import argparse
import time
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


scraper_utils = USStateLegislatorScraperUtils('AZ', 'us_az_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.azleg.gov')


def get_leg_bios(myurl):
    leg_bio = []

    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")

    house_roster = page_soup.find("table", {"id": "HouseRoster"})
    house_people = house_roster.findAll("tr")
    senate_roster = page_soup.find("table", {"id": "SenateRoster"})
    senate_people = senate_roster.findAll("tr")
    for hp in house_people[1:]:
        person_info = hp.findAll("td")
        person_link = person_info[0].a["href"]
        state_member_id = person_link.split("legislator=")[1]

        name_full = person_info[0].text
        name_full = name_full.split("--")[0].strip()
        hn = HumanName(name_full)

        district = person_info[1].text

        party = person_info[2].span["title"]
        if party == "Republican":
            party_id = 3
        elif party == "Democratic":
            party = "Democrat"
            party_id = 2
        else:
            print(party)

        email_id = person_info[3].text
        try:
            email = email_id.split(": ")[1] + '@azleg.gov'

        except:
            email = ""

        phone = person_info[5].text
        phone = phone.replace("(", "")
        phone = phone.replace(") ", "-")
        phns = []
        phone_numbers = {'office': '', 'number': phone}
        if phone != "":
            phns.append(phone_numbers)

        party_id = scraper_utils.get_party_id(party)

        leg_info = {'state_url': person_link, 'state_member_id': state_member_id, 'name_full': name_full,
                    'name_last': hn.last, 'name_first': hn.first, 'name_middle': hn.middle, 'name_suffix': hn.suffix,
                    'district': district, 'party': party, 'party_id': party_id, 'email': email,
                    'phone_numbers': phns, 'role': 'Representative'}
        leg_bio.append(leg_info)

    for hp in senate_people[1:]:
        person_info = hp.findAll("td")
        person_link = person_info[0].a["href"]
        state_member_id = person_link.split("legislator=")[1]

        name_full = person_info[0].text
        name_full = name_full.split("--")[0].strip()
        hn = HumanName(name_full)

        district = person_info[1].text

        party = person_info[2].span["title"]
        if party == "Republican":
            party_id = 3
        elif party == "Democratic":
            party = "Democrat"
            party_id = 2
        else:
            print(party)

        email_id = person_info[3].text
        try:
            email = email_id.split(": ")[1] + '@azleg.gov'

        except:
            email = ""

        phone = person_info[5].text
        phone = phone.replace("(", "")
        phone = phone.replace(") ", "-")
        phns = []
        phone_numbers = {'office': '', 'number': phone}
        if phone != "":
            phns.append(phone_numbers)

        leg_info = {'state_url': person_link, 'state_member_id': state_member_id, 'name_full': name_full,
                    'name_last': hn.last, 'name_first': hn.first, 'name_middle': hn.middle, 'name_suffix': hn.suffix,
                    'district': district, 'party': party, 'party_id': party_id, 'email': email,
                    'phone_numbers': phns, 'role': 'Senator'}
        leg_bio.append(leg_info)
    scraper_utils.crawl_delay(crawl_delay)
    return leg_bio


def collect_leg_data(myurl):
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")
    committees = []
    com_table = page_soup.find("table", {"id": "committee-table"})
    try:
        com_list = com_table.findAll("tr")

        for com in com_list[1:]:
            com_info = com.findAll("td")
            # print(com_info)
            com_name = com_info[0].text
            role = com_info[1].text
            c = {'role': role, 'committee': com_name}
            committees.append(c)
    except:
        pass
    occupation = []
    years_active = []
    most_recent_term_id = ""
    bold_info = page_soup.find("td", {"height": "99"})
    if bold_info is not None:
        strong_labels = bold_info.findAll("strong")
        for sl in strong_labels:
            if "Occupation" in sl.text:
                try:
                    occ = (sl.nextSibling).replace("<br/>", "")
                    occ = occ.split(",")
                    for o in occ:
                        if o.strip() != "":
                            # if the string is not empty
                            occupation.append(o.strip())
                except:
                    pass
            elif "Member Since" in sl.text:
                memsin = sl.text.split(":")
                joined = memsin[len(memsin) - 1].strip()
                # print(joined)
                numbers = [int(word)
                           for word in joined.split() if word.isdigit()]
                # print(numbers)
                if numbers:
                    year_started = numbers[0]
                else:
                    year_started = ""
                try:
                    years_active = list(range(int(year_started), 2021))
                    most_recent_term_id = str(
                        years_active[len(years_active) - 1])

                except:
                    years_active = []

    leg_info = {'state_url': myurl, 'committees': committees, 'seniority': None, 'areas_served': [],
                'occupation': occupation,
                'years_active': years_active, 'most_recent_term_id': most_recent_term_id, 'addresses': [],
                'military_experience': ""}
    # print(leg_info)
    scraper_utils.crawl_delay(crawl_delay)
    return leg_info


def find_reps_wiki(repLink):
    bio_lnks = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        biolink = "https://en.wikipedia.org/" + (info[1].a["href"])

        bio_lnks.append(biolink)

    return bio_lnks


def find_sens_wiki(repLink):
    bio_links = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        biolink = "https://en.wikipedia.org/" + (info[2].a["href"])

        bio_links.append(biolink)

    return bio_links


# def find_wiki_rep_data(repLink):
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         reps = page_soup.find("div", {"class": "mw-parser-output"})
#         repBirth = reps.find("span", {"class": "bday"}).text
#
#         b = datetime.datetime.strptime(repBirth, "%Y-%m-%d").date()
#
#         birthday = b
#         # print(b)
#
#
#
#
#     except:
#         # couldn't find birthday in side box
#         birthday = None
#
#     # get education
#     education = []
#     lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD", "DDS", "MS", "BS", "MBA", "MS", "MD"]
#
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         reps = page_soup.find("div", {"class": "mw-parser-output"})
#         # repsAlmaMater = reps.find("th", {"scope:" "row"})
#         left_column_tags = reps.findAll()
#         lefttag = left_column_tags[0]
#         for lefttag in left_column_tags:
#             if lefttag.text == "Alma mater" or lefttag.text == "Education":
#                 index = left_column_tags.index(lefttag) + 1
#                 next = left_column_tags[index]
#                 alines = next.findAll()
#                 for aline in alines:
#                     if "University" in aline.text or "College" in aline.text or "School" in aline.text:
#                         school = aline.text
#                         # this is most likely a school
#                         level = ""
#                         try:
#                             lineIndex = alines.index(aline) + 1
#                             nextLine = alines[lineIndex].text
#                             if re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
#                                 level = nextLine
#                         except:
#                             pass
#
#                     edinfo = {'level': level, 'field': "", 'school': school}
#
#                     if edinfo not in education:
#                         education.append(edinfo)
#
#     except Exception as ex:
#
#         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#
#         message = template.format(type(ex).__name__, ex.args)
#
#         # print(message)
#
#     # get full name
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         head = page_soup.find("h1", {"id": "firstHeading"})
#         name = head.text
#         name = name.replace(" (politician)", "")
#         name = name.replace(" (American politician)", "")
#         name = name.replace(" (North Carolina politician)", "")
#
#
#     except:
#         name = ""
#     name = unidecode.unidecode(name)
#
#     hN = HumanName(name)
#
#     info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
#             'education': education}
#
#     # print(info)
#     return info


if __name__ == '__main__':
    memberroster = 'https://www.azleg.gov/memberroster/'
    leg_roster_info = get_leg_bios(memberroster)
    leg_roster_df = pd.DataFrame(leg_roster_info)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    leg_indv_links = leg_roster_df['state_url']
    # less_links = leg_indv_links[:10]

    with Pool() as pool:
        leg_data = pool.map(func=collect_leg_data, iterable=leg_indv_links)
    leg_df = pd.DataFrame(leg_data)

    leg_df = pd.merge(leg_df, leg_roster_df, how='left', on=['state_url'])

    wiki_rep_link = 'https://en.wikipedia.org/wiki/Arizona_House_of_Representatives'
    wiki_sen_link = 'https://en.wikipedia.org/wiki/Arizona_Senate'

    reps_wiki = find_reps_wiki(wiki_rep_link)

    sens_wiki = find_sens_wiki(wiki_sen_link)

    all_wiki_links = reps_wiki
    for sw in sens_wiki:

        all_wiki_links.append(sw)

    # with Pool() as pool:
    #     wiki_data = pool.map(func=find_wiki_rep_data, iterable=all_wiki_links)
    # wiki_df = pd.DataFrame(wiki_data)

    with Pool() as pool:

        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_first", "name_last"])

    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})

    sample_row = scraper_utils.initialize_row()
    # print(sample_row)
    #

    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id
    #
    #
    big_df['source_url'] = big_df['state_url']
    big_df['source_id'] = big_df['state_member_id']
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id

    print(big_df)

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
