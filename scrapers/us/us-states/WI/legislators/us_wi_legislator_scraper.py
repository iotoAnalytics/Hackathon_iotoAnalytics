'''
This website will block your IP if too many requests are sent
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from requests import get
from geotext import GeoText
import re
import datetime
from multiprocessing import Pool
import unidecode
import datefinder
import requests
from nameparser import HumanName
import psycopg2
from bs4 import BeautifulSoup as soup
from urllib.request import Request
from urllib.request import urlopen as uReq
import bs4
import pandas as pd
import time
import argparse
import gzip
import numpy as np
import pickle
from scraper_utils import USStateLegislatorScraperUtils
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# import html.parser

scraper_utils = USStateLegislatorScraperUtils('WI', 'us_wi_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://docs.legis.wisconsin.gov')


def getSenateLinks(myurl):
    if "senate" in myurl:
        role = 'Senator'
    elif "assembly" in myurl:
        role = 'Representative'
    infos = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    scraper_utils.crawl_delay(crawl_delay)

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")
    people_infos = page_soup.findAll("span", {"class": "info"})
    email_infos = page_soup.findAll("span", {'class': 'info email'})
    emails = []
    for ei in email_infos:
        emails.append(ei.a.text)

    i = 0
    total_info_odds = page_soup.findAll("div", {"class", "rounded odd"})
    total_info_evens = page_soup.findAll("div", {"class", "rounded even"})
    people_infos = total_info_evens
    for toi in total_info_odds:
        people_infos.append(toi)
    # print(len(people_infos))
    for pi in people_infos:
        email = ""
        try:
            person_info = pi.find("span", {"class": "info"})
            state_url = person_info.span.strong.a["href"]
            if "https" in state_url:
                name = person_info.span.strong.a.text
                hn = HumanName(name)
                party_area = person_info.span.small.text
                party_area = party_area.split(" - ")
                if "D" in party_area[0]:
                    party = "Democrat"

                elif "R" in party_area[0]:
                    party = "Republican"

                party_id = scraper_utils.get_party_id(party)
                areas_served = []
                areas_served.append(party_area[1].replace(")", ""))

                district_tags = person_info.findAll(
                    "span", {"style": "width:8em;"})
                district = ""
                state_member_id = ""
                for dt in district_tags:
                    try:
                        if "District" in dt.small.text:
                            district = dt.text.replace("District ", "")
                            index = district_tags.index(dt) + 1
                            state_url = 'https://docs.legis.wisconsin.gov' + \
                                (district_tags[index].a["href"])
                            if role == "Senator":
                                state_member_id = state_url.split("senate/")[1]
                            else:

                                state_member_id = state_url.split(
                                    "assembly/")[1]

                    except Exception as ex:

                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

                        message = template.format(type(ex).__name__, ex.args)

                        # print(message)

                email_info = pi.find("span", {"class": "info email"})
                email = email_info.a.text
                phone_numbers = []

                telephone_info = pi.find("span", {"class": "info telephone"})
                numbers = telephone_info.text.replace("Telephone:", "")
                numbers = numbers.split("(")
                for number in numbers:
                    number = number.replace(")", "").strip()
                    if number:
                        pn = {'office': "", 'number': number.replace(" ", "-")}
                        phone_numbers.append(pn)

                info = {'state_url': state_url, 'name_full': name, 'name_first': hn.first, 'name_last': hn.last,
                        'name_middle': hn.middle, 'name_suffix': hn.suffix, 'party': party, 'party_id': party_id,
                        'areas_served': areas_served, 'state': "WI", 'state_id': 55, 'country': 'USA', 'country_id': 1,
                        'role': role, 'district': district, 'state_member_id': state_member_id, 'email': email,
                        'phone_numbers': phone_numbers}

                infos.append(info)

        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"

            message = template.format(type(ex).__name__, ex.args)

            # print(message)
        i += 1

    # print(infos)
    # print(len(infos))

    return infos


def collect_leg_data(myurl):
    addresses = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()
    scraper_utils.crawl_delay(crawl_delay)
    uReq(req).close()

    page_soup = soup(webpage, "html.parser")

    office_info = page_soup.find("span", {"class": "info office"})

    office_address = office_info.text.replace("\n", " ")
    office_address = " ".join(office_address.split())
    office_address = ", PO".join(office_address.split("PO"))
    office_address = ", Madison".join(office_address.split("Madison"))
    office_address = ", State".join(office_address.split("State"))
    office_address = office_address.strip()

    addr_info = {'location': 'Madison Office', 'address': office_address}
    addresses.append(addr_info)
    try:
        voting_address_info = page_soup.find(
            "span", {"class": "info voting_address"})
        voting_addr = str(voting_address_info)
        voting_addr = voting_addr.split("span")[3]
        voting_addr = voting_addr.replace(">", "")
        voting_addr = voting_addr.replace("<br/", " ")
        voting_addr = voting_addr.replace("</", "")
        voting_addr = voting_addr.replace("\n", " ")
        voting_addr = " ".join(voting_addr.split())
        addr_info = {'location': 'Voting Address',
                     'address': voting_addr.strip()}
        # print(addr_info)
        addresses.append(addr_info)

    except:
        pass
    committees = []
    try:
        committee_info = page_soup.find("div", {"id": "committees"})
        all_comms = committee_info.findAll("li")
        for c in all_comms:
            role = ""
            committee = c.text.strip()
            if "(" in committee:
                role = committee.split("(")[1].replace(")", "")
                committee = committee.split("(")[0]
            com_info = {'role': role, 'committee': committee}
            committees.append(com_info)
    except:
        pass

    info = {'state_url': myurl, 'addresses': addresses, 'committees': committees}

    return info


def get_senate_wiki_links(repLink):
    bio_links = []
    uClient = uReq(repLink)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    for person in people[1:]:
        try:
            info = person.findAll("td")
            info = info[1]
            biolink = "https://en.wikipedia.org" + \
                (info.span.span.span.a["href"])

            bio_links.append(biolink)
            # print(biolink)
        except:
            pass

    return bio_links


def get_house_wiki_links(repLink):
    bio_lnks = []
    uClient = uReq(repLink)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[4].findAll("tr")
    i = 0
    for person in people[1:]:
        try:
            info = person.findAll("td")
            if i % 3 == 0:
                biolink = "https://en.wikipedia.org/" + (info[2].a["href"])
            else:
                biolink = "https://en.wikipedia.org/" + (info[1].a["href"])

            bio_lnks.append(biolink)

        except:
            pass
        i += 1

    return bio_lnks


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "lxml")
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


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Wisconsin_State_Assembly"
    wikipage_senate = "https://ballotpedia.org/Wisconsin_State_Senate"

    if row.role == "Representative":
        try:
            uClient = uReq(wikipage_reps)
            page_html = uClient.read()
            uClient.close()

            page_soup = soup(page_html, "lxml")
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

            page_soup = soup(page_html, "lxml")
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


def find_wiki_data(repLink):
    try:
        uClient = uReq(repLink)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})
        repBirth = reps.find("span", {"class": "bday"}).text

        b = datetime.datetime.strptime(repBirth, "%Y-%m-%d").date()

        birthday = b
        # print(b)

    except:
        # couldn't find birthday in side box
        birthday = None

    # get years_active, based off of "assumed office"
    years_active = []
    year_started = ""
    try:
        uClient = uReq(repLink)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        table = page_soup.find("table", {"class": "infobox vcard"})

        tds = table.findAll("td", {"colspan": "2"})
        td = tds[0]

        for td in tds:
            asof = (td.find("span", {"class": "nowrap"}))
            if asof != None:
                if (asof.b.text) == "Assumed office":

                    asofbr = td.find("br")

                    year_started = (asofbr.nextSibling)

                    year_started = year_started.split('[')[0]
                    if "," in year_started:
                        year_started = year_started.split(',')[1]
                    year_started = (year_started.replace(" ", ""))
                    year_started = re.sub('[^0-9]', '', year_started)
                    if year_started.startswith("12"):
                        year_started = year_started.substring(1)

                else:
                    pass

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        # print(message)

    if year_started != "":
        years_active = list(range(int(year_started), 2021))
        # years_active_lst.append(years_active_i)
    else:
        years_active = []
        # years_active_i = []
        # years_active_i.append(years_active)
        # years_active_lst.append(years_active_i)

    # get education
    education = []
    lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD",
            "DDS", "MS", "BS", "MBA", "MS", "MD"]

    try:
        uClient = uReq(repLink)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})
        # repsAlmaMater = reps.find("th", {"scope:" "row"})
        left_column_tags = reps.findAll()
        lefttag = left_column_tags[0]
        for lefttag in left_column_tags:
            if lefttag.text == "Alma mater" or lefttag.text == "Education":
                index = left_column_tags.index(lefttag) + 1
                next = left_column_tags[index]
                alines = next.findAll()
                for aline in alines:
                    if "University" in aline.text or "College" in aline.text or "School" in aline.text:
                        school = aline.text
                        # this is most likely a school
                        level = ""
                        try:
                            lineIndex = alines.index(aline) + 1
                            nextLine = alines[lineIndex].text
                            if re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
                                level = nextLine
                        except:
                            pass

                    edinfo = {'level': level, 'field': "", 'school': school}

                    if edinfo not in education:
                        education.append(edinfo)

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)

        # print(message)

    # get full name
    try:
        uClient = uReq(repLink)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        head = page_soup.find("h1", {"id": "firstHeading"})
        name = head.text
        name = name.replace(" (politician)", "")
        name = name.replace(" (American politician)", "")
        name = name.replace(" (Wisconsin politician)", "")

    except:
        name = ""
    name = unidecode.unidecode(name)

    hN = HumanName(name)

    # get occupation
    occupation = []

    try:
        uClient = uReq(repLink)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})

        left_column_tags = reps.findAll()
        lefttag = left_column_tags[0]
        for lefttag in left_column_tags:
            if lefttag.text == "Occupation":
                index = left_column_tags.index(lefttag) + 1
                occ = left_column_tags[index].text
                if occ != "Occupation":
                    occupation.append(occ)

    except:
        pass

    most_recent_term_id = ""
    try:
        most_recent_term_id = (years_active[len(years_active) - 1])

    except:
        pass

    info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
            'education': education, 'occupation': occupation, 'years_active': years_active,
            'most_recent_term_id': most_recent_term_id}

    # print(info)
    return info


senatelink = 'https://docs.legis.wisconsin.gov/2021/legislators/senate'
houselink = 'https://docs.legis.wisconsin.gov/2021/legislators/assembly'

senatepeople = getSenateLinks(senatelink)

housepeople = getSenateLinks(houselink)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
senate_info_df = pd.DataFrame(senatepeople)
# print(senate_info_df)
# print(senate_info_df)
house_info_df = pd.DataFrame(housepeople)
# print(house_info_df)
leg_info_df = (house_info_df.append(senate_info_df, sort=True))

sen_detail_links = senate_info_df["state_url"]
house_detail_links = house_info_df["state_url"]
leg_detail_links = leg_info_df["state_url"]


# print(sen_detail_links)
# print(house_detail_links)
if __name__ == '__main__':
    with Pool() as pool:
        leg_data = pool.map(func=collect_leg_data, iterable=leg_detail_links)
    leg_df = pd.DataFrame(leg_data)
    # print(leg_df)

    leg_df = pd.merge(leg_df, leg_info_df, how='left', on=['state_url'])

    print(leg_df)

    senate_wiki = 'https://en.wikipedia.org/wiki/Wisconsin_State_Senate'
    senate_wiki_links = get_senate_wiki_links(senate_wiki)
    house_wiki = 'https://en.wikipedia.org/wiki/Wisconsin_State_Assembly'
    house_wiki_links = get_house_wiki_links(house_wiki)
    # print(len(house_wiki_links))
    # print(senate_wiki_links)

    leg_wiki_links = house_wiki_links
    for swl in senate_wiki_links:
        leg_wiki_links.append(swl)

    # print(len(leg_wiki_links))

    with Pool() as pool:
        rep_data = pool.map(
            func=scraper_utils.scrape_wiki_bio, iterable=leg_wiki_links)
    wiki_df = pd.DataFrame(rep_data)
    # print(wiki_df)
    #
    mergedRepsData = pd.merge(leg_df, wiki_df, how='left', on=[
                              "name_first", "name_last"])
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

    sample_row = scraper_utils.initialize_row()
    # print(sample_row)
    #
    big_df = mergedRepsData
    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id

    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id
    big_df['seniority'] = None
    big_df['military_experience'] = ""
    big_df['source_url'] = big_df['state_url']
    big_df['source_id'] = big_df['state_member_id']
    big_df['seniority'] = 0
    get_wiki_url(sample_row)

    try:
        gender = scraper_utils.get_legislator_gender(big_df['name_first'], big_df['name_last'])
        if not gender:
            gender = 'O'
        sample_row.gender = gender
    except:
        pass



    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Wisconsin_State_Assembly"
    wikipage_senate = "https://ballotpedia.org/Wisconsin_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    ballot_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    bigger_df = pd.merge(big_df, ballot_df, how='left',
                      on=["name_last", 'wiki_url'])

    isna = bigger_df['education'].isna()
    bigger_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    bigger_df['birthday'] = bigger_df['birthday'].replace({np.nan: None})
    bigger_df['wiki_url'] = bigger_df['wiki_url'].replace({np.nan: None})

    big_list_of_dicts = bigger_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
