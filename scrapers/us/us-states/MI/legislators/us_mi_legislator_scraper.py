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
from unidecode import unidecode
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
from geotext import GeoText
from requests import get
import ssl
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

ssl._create_default_https_context = ssl._create_unverified_context


# import html.parser

scraper_utils = USStateLegislatorScraperUtils('MI', 'us_mi_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.house.mi.gov')


def decodeEmail(e):
    de = ""
    k = int(e[:2], 16)

    for i in range(2, len(e) - 1, 2):
        de += chr(int(e[i:i + 2], 16) ^ k)

    return de


def get_wiki_url(role, name_last, district):

    wikipage_reps = "https://ballotpedia.org/Michigan_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Michigan_State_Senate"

    if role == "Representative":
        uClient = uReq(wikipage_reps)
    elif role == "Senator":
        uClient = uReq(wikipage_senate)

    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "lxml")
    table = page_soup.find("table", {"id": 'officeholder-table'})
    rows = table.findAll("tr")

    for person in rows[1:]:
        tds = person.findAll("td")
        name_td = tds[1]
        name = name_td.text
        name = name.replace('\n', '')
        name = HumanName(name)

        district_td = tds[0]
        district = district_td.text
        district_num = re.search(r'\d+', district).group().strip()

        if name.last == name_last and district_num == district:
            link = name_td.a['href']
            return link


def get_sen_bio(myurl):
    links = []
    uClient = uReq(myurl)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    senator_data = []
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    sen_info = page_soup.findAll("div", {"class": "right"})

    for si in sen_info:
        party = ""
        name_party = si.h2.a.text
        url = si.h2.a["href"]
        if url == "/horn" or url == "/zorn" or url == "/stamas" or url == "/schmidt" or url == "/macgregor":
            url = 'http://www.senate.michigan.gov' + url

        name = name_party.split("(")[0]
        name = name.strip()
        name = unidecode.unidecode(name)

        hN = HumanName(name)

        if "(R)" in name_party:
            party = "Republican"
            party_id = scraper_utils.get_party_id(party)
        elif "(D)" in name_party:
            party = "Democrat"
            party_id = scraper_utils.get_party_id(party)
        else:
            party = "Other"
            party_id = 0

        das = si.findAll("a")
        phone_numbers = []
        for da in das:
            # find district
            if "District" in da.text:
                district_info = da.text
                district_info = district_info.split(" ")
                district = district_info[2]
                if district == "Map":
                    district = 0
            # find phone_number
            if "Phone" in da.text:
                number = (da.text).replace("Phone: ", "")
                pn = {'office': "", 'number': number}
                phone_numbers.append(pn)

        # get addresses
        addresses = []
        alltags = si.findAll()
        lasttag = alltags[len(alltags) - 1]

        address_info = lasttag.nextSibling.replace("Office: ", "")
        address_info = address_info.strip()
        location = "office"
        addr = {'location': location, "address": address_info}
        addresses.append(addr)
        gender = scraper_utils.get_legislator_gender(hN.first, hN.last)
        wiki_url = get_wiki_url("Senator", hN.last, district)

        sd = {'source_url': str(url).strip(), 'name_full': str(hN), 'name_first': hN.first, 'name_last': hN.last,
              'name_middle': hN.middle, 'name_suffix': hN.suffix, 'party': party, 'party_id': party_id,
              'district': district, 'role': 'Senator', 'phone_numbers': phone_numbers, 'addresses': addresses, 'gender': gender, "wiki_url": wiki_url}
        senator_data.append(sd)

    return senator_data


def get_rep_bio(myurl):
    print(myurl)
    driver = webdriver.Chrome(ChromeDriverManager().install())

    rep_bios = []

    driver.get(myurl)
    all_reps = driver.find_elements_by_class_name("fa-chevron-circle-down")
    all_reps[2].click()
    time.sleep(2)

    html = driver.page_source
    s = soup(html, 'lxml')
    body = s.find("div", {'class': 'card-body'})
    items = body.find_all('li')
    state_url = ''
    for item in items:
        #print(item.text)
        links = item.find_all('a')
        for l in links:
            if "tel" not in l.get('href') and 'mailto' not in l.get('href') and '.pdf' not in l.get('href'):
                 state_url = l.get('href')
        try:
            rows = item.find_all('div')

            name = rows[0].text
            name = name.split(' (')[0].replace('\n', '').replace('  ', '')
            if name != "Vacant":
                party = rows[0].text
                party = party.split(')')[0].replace('\n', '').replace('  ', '')
                party = party.split('(')[1]
                party_id = scraper_utils.get_party_id(party)
                district = rows[0].text
                district = district.split('-')[1].replace('\n', '').replace('  ', '')
                address = rows[3].text
                address = address.strip()
                addresses = {"location": "office", "address": address}
                email = rows[5].text
                email = email.strip()
                inner_row = rows[2].find_all('div')
                phone = inner_row[1].text
                phone = phone.replace('\n', '').replace('  ', '')
                phone = phone.replace('(', '').replace(')', '').replace('+1 ', '').replace(' ', '-')

                hn = HumanName(name)
                gender = scraper_utils.get_legislator_gender(hn.first, hn.last)
                wiki_url = get_wiki_url("Representative", hn.last, district)

                bio_info = {'source_url': state_url, 'district': district, 'name_full': hn.full_name, 'name_first': hn.first,
                            'name_last': hn.last, 'name_middle': hn.middle, 'name_suffix': hn.suffix, 'party': party,
                            'party_id': party_id, 'phone_numbers': phone, 'email': email,
                            'role': 'Representative', "addresses": addresses,
                            'state': 'MI', 'state_id': 26, 'country': 'United States of America', 'country_id': 1, 'gender': gender, "wiki_url": wiki_url}
                rep_bios.append(bio_info)
        except:
            pass

    driver.close()
    return rep_bios


def collect_sen_data(myurl):
    years_active = 0
    committees = []
    areas_served = []
    email = ""
    try:
        # get senator years_active, committees
        # if democrat
        if "senatedems" in myurl:
            years_active = []
            uClient = uReq(myurl)
            scraper_utils.crawl_delay(crawl_delay)
            page_html = uClient.read()
            uClient.close()
            # # html parsing

            page_soup = soup(page_html, "html.parser")

            # print("DEMOCRAT")
            sen_about = page_soup.find("div", {"class": "senator_about_info"})
            about_left = sen_about.findAll()
            for al in about_left:
                if al.text == "Terms Elected:":

                    terms_elected = al.nextSibling.split(',')
                    for te in terms_elected:
                        term = te.split("(")[0].strip()
                        term = int(term)
                        years_active.append(term)
                        years_active.append(term + 1)

                    # term they were elected and years after because terms in ga senate are two years
                    # print(years_active)
                if al.text == "Area Represented:":

                    areas = al.nextSibling
                    areas = areas.replace(" and includes: ", ", ")
                    areas = areas.split(", ")
                    for a in areas:
                        a = a.strip()
                        areas_served.append(a.replace("located in ", ""))
                if al.text == "Committees:":

                    committee_list = al.nextSibling
                    committee_list = committee_list.split(",")

                    for cl in committee_list:
                        cl = cl.strip()
                        com_item = {'role': 'member', 'committee': cl}
                        committees.append(com_item)

            # get email

            contact_info = page_soup.find("div", {"class": "left"})
            contacts = contact_info.text
            contacts = contacts.split(" ")
            contact_info = []
            for c in contacts:
                contact_info.append(c.replace("\n", ""))

            while "" in contact_info:
                contact_info.remove("")
            for c in contact_info:
                c.strip()
            email = contact_info[0]

        else:
            # republican
            req = Request(myurl,
                          headers={'User-Agent': 'Mozilla/5.0'})
            webpage = uReq(req).read()
            scraper_utils.crawl_delay(crawl_delay)

            uReq(req).close()

            page_soup = soup(webpage, "html.parser")

            bio_paragraph = page_soup.find(
                "div", {"class": "elementor-text-editor elementor-clearfix"})
            bio_p = bio_paragraph.findAll("p")
            for bp in bio_p:
                try:
                    email_protected = bp.a["href"]
                    # print(email_protected)
                    email_protected = email_protected.split("protection#")[1]
                    # print(email_protected)
                    email = (decodeEmail(email_protected))
                    email = email.replace("Edit mailto:", "")
                    # print(email)
                except Exception as ex:

                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"

                    message = template.format(type(ex).__name__, ex.args)
                    # print(message)
                if "District" in bp.text:

                    # print(t)
                    bptextgt = GeoText(bp.text)
                    for city in bptextgt.cities:
                        if city != "Michigan":
                            areas_served.append(city)
                    a = bp.text.split("District")[1]

                    a = a.split(".")[0]
                    t = re.findall('([A-Z][a-z]+)', a)
                    for area in t:
                        cont = 0
                        for areser in areas_served:
                            if area in areser:
                                cont = 1
                        if cont == 0 and "Township" not in area and "Count" not in area and "St" not in area:
                            if "Mt" not in area and "Lake" not in area:
                                areas_served.append(area)
            # print(areas_served)

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)
        # print(message)

    sen_info = {'source_url': myurl, 'email': email, 'years_active': years_active, 'areas_served': areas_served,
                'state': 'MI', 'state_id': 26, 'country': 'United States of America',
                'country_id': 1, 'seniority': "", 'military_experience': "", 'source_id': ""}
    return sen_info


def collect_rep_data(myurl):
    addresses = []
    #

    if "gophouse.org/representatives/" in myurl:
        # common url format for republican websites

        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': 'https://cssspritegenerator.com',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
        d = get(myurl, headers=hdr)
        page_soup = soup(d.content, 'html.parser')
        # get addresses
        try:

            contact = page_soup.find(
                "div", {"class": "font-condensed text-15"})
            contact_lists = contact.findAll("p")
            # i = 1
            for c in contact_lists:
                try:
                    office = c.strong.text
                    if "Phone" not in office and "Email" not in office and "Facebook" not in office:
                        location = office

                        ind = contact_lists.index(c) + 1
                        addy = (contact_lists[ind]).text
                        addy = addy.replace("\xa0", "")

                        address_info = {'address': addy.replace(
                            "\n", ", "), 'location': location}

                        addresses.append(address_info)
                except:
                    pass

        except:
            pass

    elif "/housedems.com/" in myurl:
        # common url format for democrat websites
        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': 'https://cssspritegenerator.com',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
        d = get(myurl, headers=hdr)
        page_soup = soup(d.content, 'html.parser')
        # get addresses
        get_in_touch = page_soup.find(
            "div", {"class": "fusion-text fusion-text-3"})
        lis = get_in_touch.findAll("li")
        address = lis[2].text.strip()

        addr_info = {'address': address.replace(
            "\xa0", ""), 'location': 'office'}
        addresses.append(addr_info)

    rep_info = {'source_url': myurl, 'addresses': addresses}
    return rep_info


def get_house_committee_info(myurl):
    print(myurl)

    driver = webdriver.Chrome(ChromeDriverManager().install())

    committee_info = []
    committee_links = []

    driver.get(myurl)
    all_committees = driver.find_element_by_class_name("card-body")
    committee_list = all_committees.find_elements_by_tag_name('li')
    for c in committee_list:
        link = c.find_element_by_tag_name('a').get_attribute('href')
        committee_links.append(link)

    for link in committee_links:
        driver.get(link)
        html = driver.page_source
        s = soup(html, 'lxml')

        com_name = s.find_all('h3')[1].text
        list = s.find('div', {"class": "col-sm-6"})
        memberships = list.find_all('li')
        for m in memberships:
            name = m.text
            if "Vice Chair" in name:
                role = "Vice Chair"
            elif "Chair" in name:
                role = "Chair"
            else:
                role = "Member"
            name = name.split('. ')[1]
            name = name.split(' (')[0]
            hn = HumanName(name)
            name_last = hn.last
            name_first = hn.first
            role_and_committee = {"role": role, 'committee': com_name}
            comm_person = {'name_last': name_last,'name_first': name_first, 'committees': role_and_committee}
            committee_info.append(comm_person)
    driver.close()
    return committee_info



def get_rep_wiki_links(repLink):
    wiki_info = []
    uClient = uReq(repLink)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    tbl = page_soup.findAll("tbody")
    reps_table = tbl[2]

    atbl = reps_table.findAll("td")
    atbl = atbl[1:]
    for td in atbl:
        try:
            link = (td.a["href"])
            if "Michigan" not in link and "#" not in link:
                repLink = "https://en.wikipedia.org" + link
                ind = atbl.index(td)
                areas_served = (atbl[ind + 2].text).split(", ")
                wi = {'wiki_link': repLink, 'areas_served': areas_served}
                wiki_info.append(wi)
                # print(wi)

        except:
            pass

    return wiki_info


def scrape_wiki_bio_Links(wikiUrl):
    repLinks = []
    uClient = uReq(wikiUrl)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    # #
    # # #grabs each product
    reps = page_soup.find("div", {"class": "mw-parser-output"})

    reps = reps.find("table", {"class": "wikitable sortable"})
    atags = reps.findAll("a")
    ind = 1
    for atag in atags:
        if ind % 3 == 2:
            repLink = "https://en.wikipedia.org" + (atag["href"])
            repLinks.append(repLink)
        ind = ind + 1

    return repLinks


def get_committee_urls(myurl):
    committee_urls = []
    uClient = uReq(myurl)
    scraper_utils.crawl_delay(crawl_delay)
    page_html = uClient.read()
    uClient.close()
    # # html parsing

    page_soup = soup(page_html, "html.parser")

    sections = page_soup.findAll("div", {"class": "col-md-6"})
    sec = sections[0]
    lis = sec.findAll("li")
    for l in lis:
        comm = l.a["href"]
        comm_url = "https://committees.senate.michigan.gov" + comm
        committee_urls.append(comm_url)
    sec = sections[1]
    lis = sec.findAll("li")
    for l in lis:
        comm = l.a["href"]
        comm_url = "https://committees.senate.michigan.gov" + comm
        committee_urls.append(comm_url)
    sec = sections[2]
    lis = sec.findAll("li")
    for l in lis:
        comm = l.a["href"]
        comm_url = "https://committees.senate.michigan.gov" + comm
        committee_urls.append(comm_url)
    return committee_urls


def get_committee_info(myurl):
    committee_info = []
    try:
        uClient = uReq(myurl)
        scraper_utils.crawl_delay(crawl_delay)
        page_html = uClient.read()
        uClient.close()
        # # html parsing

        page_soup = soup(page_html, "html.parser")

        titlecol = page_soup.find("div", {"class": "col-sm-8"})
        com_name = titlecol.h2.span.text
        # print(com_name)

        all_members = page_soup.find("ul", {"id": "MainContent_BLMembers"})
        all_members = all_members.findAll("li")

        for memb in all_members:
            state_url = memb.a["href"]

            # print(state_url)
            name_role = memb.text
            # print(name_role)
            name = name_role.split("(")[0]
            name = name.strip()
            hn = HumanName(name)
            name_last = hn.last

            role = name_role.split(")")[1]
            role = role.strip()
            # print(role)

            role_and_committee = {"role": role, 'committee': com_name}
            # role_and_committee = empty_list.append(role_and_committee)
            comm_person = {'name_last': name_last,
                           'committees': role_and_committee}
            committee_info.append(comm_person)
    except:
        pass

    # print(committee_info)

    return committee_info


def wiki_rep_areas(area_link_dict):
    wiki_dict = scraper_utils.scrape_wiki_bio(area_link_dict['wiki_link'])
    wiki_dict['areas_served'] = area_link_dict['areas_served']
    wiki_dict['seniority'] = 0
    wiki_dict['military_experience'] = ""
    return wiki_dict


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


if __name__ == '__main__':
    sen_page = 'https://senate.michigan.gov/senatorinfo_complete.html'
    # house_page = 'https://www.house.mi.gov/MHRPublic/frmRepListMilenia.aspx?all=true'
    house_page = 'https://www.house.mi.gov/AllRepresentatives'
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # get links for each person's page
    sen_bio = get_sen_bio(sen_page)
    bio_df = pd.DataFrame(sen_bio)
    sen_links = [(d['source_url']) for d in sen_bio]
    sen_party = [(d['party']) for d in sen_bio]
    # print(sen_links)
    # print(len(sen_links))

    # with Pool() as pool:
    #     sen_data = pool.map(func=collect_sen_data, iterable=sen_links)
    sen_data = [collect_sen_data(url) for url in sen_links]
    sendata = []
    print("pool 1")
    for sl in sen_links:
        sendata.append(collect_sen_data(sl))
    # print(sendata)
    sendf = pd.DataFrame(sen_data)

    # print(sendf)
    sendf = pd.merge(sendf, bio_df, how='left', on=['source_url'])
    # print("sendf")
    # print(sendf)

    # get committee info
    committee_url = 'https://committees.senate.michigan.gov/'
    committees = get_committee_urls(committee_url)
    # print(committees)

    # with Pool() as pool:
    #
    #     commData = pool.map(func=get_committee_info, iterable=committees)
    commData = [get_committee_info(url) for url in committees]
    # print(commData)
    print("pool 2")
    comm_list = []
    for sublist in commData:
        for item in sublist:
            contained = 0
            for dict in comm_list:

                if dict["name_last"] == item["name_last"]:
                    # print(dict["committees"])

                    k = (item["committees"])
                    com_info_list = dict["committees"]
                    com_info_list.append(k)
                    # print(comm_info_list)

                    dict['committees'] = com_info_list

                    contained = 1

            if contained == 0:
                su = item["name_last"]
                coms = [item["committees"]]
                item = {'name_last': su, 'committees': coms}
                # print(item)
                comm_list.append(item)

            # if item["state_url"] not in comm_list['state_url']:
            #     comm_list.append(item)
            # elif item.state_url in comm_list:

    comm_df = pd.DataFrame(comm_list)

    sendf = pd.merge(sendf, comm_df, how='left', on=["name_last"])

    # get senate info from wikipedia
    # wiki_senate_link = 'https://en.wikipedia.org/wiki/Michigan_Senate'
    # wikiData = scrape_wiki_bio_Links(wiki_senate_link)
    #
    # with Pool() as pool:
    #
    #     wikiData = pool.map(
    #         func=scraper_utils.scrape_wiki_bio, iterable=wikiData)
    # wikidf = pd.DataFrame(wikiData)
    # print("pool 3")
    # # print("wikidf")
    #
    # wikidf = wikidf.rename(columns={'years_active': 'years_active_wiki'})
    #
    # mergedSensData = pd.merge(sendf, wikidf, how='left', on=[
    #                           "name_first", "name_last"])
    # # print(mergedSensData)
    #
    # mergedSensData['years_active'] = np.where(mergedSensData['years_active'] == 0, mergedSensData['years_active_wiki'],
    #                                           mergedSensData['years_active'])
    #
    # mergedSensData = mergedSensData.drop(labels="years_active_wiki", axis=1)
    # mergedSensData['birthday'] = mergedSensData['birthday'].replace({
    #                                                                 np.nan: None})
    # mergedSensData['occupation'] = mergedSensData['occupation'].replace({
    #                                                                     np.nan: None})
    # mergedSensData['most_recent_term_id'] = mergedSensData['most_recent_term_id'].replace({
    #                                                                                       np.nan: None})
    # mergedSensData['years_active'] = mergedSensData['years_active'].replace({
    #                                                                         np.nan: None})
    # mergedSensData['education'] = mergedSensData['education'].replace({
    #                                                                   np.nan: None})
    # print(mergedSensData)

    rep_bio = get_rep_bio(house_page)
    rep_bio_df = pd.DataFrame(rep_bio)

    comm_link = 'https://www.house.mi.gov/StandingCommittees'
    commData = get_house_committee_info(comm_link)
    comm_list = []

    for item in commData:
        contained = 0
        for dict in comm_list:

            if dict["name_last"] == item["name_last"] and dict["name_first"] == item["name_first"]:

                k = (item["committees"])
                com_info_list = dict["committees"]
                com_info_list.append(k)
                # print(comm_info_list)

                dict['committees'] = com_info_list

                contained = 1

        if contained == 0:
            su = item["name_last"]
            fn = item["name_first"]
            coms = [item["committees"]]
            item = {'name_last': su, 'name_first': fn, 'committees': coms}
            # print(item)
            comm_list.append(item)

    # print(*comm_list, sep="\n")
    comm_df = pd.DataFrame(comm_list)
    print(comm_df)
    #
    rep_df = pd.merge(rep_bio_df, comm_df, how='left',
                      on=["name_last", "name_first"])

    # rep_wiki_link = 'https://en.wikipedia.org/wiki/Michigan_House_of_Representatives'
    # wiki_links_areas = get_rep_wiki_links(rep_wiki_link)
    # print(wiki_links_areas)
    # wla_rep_df = pd.DataFrame(wiki_links_areas)
    # wiki_links = wla_rep_df['wiki_url']
    # wla_rep_df['seniority'] = 0
    # wla_rep_df['military_experience'] = ""
    # print("passed")
    # rep_data = [wiki_rep_areas(url) for url in wiki_links_areas]
    # # with Pool() as pool:
    # #     rep_data = pool.map(func=wiki_rep_areas, iterable=wiki_links_areas)
    # wiki_df = pd.DataFrame(rep_data)

    # mergedRepsData = pd.merge(rep_df, wiki_df, how='left', on=[
    #                           "name_first", "name_last"])
    #
    # mergedRepsData['committees'] = mergedRepsData['committees'].replace({np.nan: None})
    # mergedRepsData['seniority'] = mergedRepsData['seniority'].replace({np.nan: None})
    # mergedRepsData['military_experience'] = mergedRepsData['military_experience'].replace({np.nan: None})
    # mergedRepsData['areas_served'] = mergedRepsData['areas_served'].replace({np.nan: None})
    # mergedRepsData['most_recent_term_id'] = mergedRepsData['most_recent_term_id'].replace({np.nan: None})
    # mergedRepsData['years_active'] = mergedRepsData['years_active'].replace({np.nan: None})
    # mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({np.nan: None})
    # mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({np.nan: None})
    # mergedRepsData['education'] = mergedRepsData['education'].replace({np.nan: None})
    # mergedRepsData['committees'] = mergedRepsData['committees'].replace({np.nan: None})
    #
    # mergedRepsData["source_id"] = ""

    # print(mergedRepsData)

    big_df = (sendf.append(rep_df, sort=True))
    big_df['seniority'] = None

    big_df = big_df[big_df['party_id'] != 0]
    print("Print something")
    wikipage_reps = "https://ballotpedia.org/Michigan_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Michigan_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))
    print("before ballotpedia call")
    with Pool() as pool:
        ballot_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    ballot_df = pd.DataFrame(ballot_data)[
        ['name_last', 'wiki_url']]

    new_df = pd.merge(big_df, ballot_df, how='left',
                      on=["name_last", 'wiki_url'])

    new_df.drop(new_df.index[big_df['wiki_url'] == ''], inplace=True)

    # isna = new_df['education'].isna()
    # new_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    new_df['committees'] = new_df['committees'].replace({np.nan: None})
    new_df['seniority'] = new_df['seniority'].replace({np.nan: None})
    new_df['military_experience'] = new_df['military_experience'].replace({np.nan: None})
    new_df['areas_served'] = new_df['areas_served'].replace({np.nan: None})
    #new_df['most_recent_term_id'] = new_df['most_recent_term_id'].replace({np.nan: None})
    new_df['years_active'] = new_df['years_active'].replace({np.nan: None})
    #new_df['occupation'] = new_df['occupation'].replace({np.nan: None})
    #new_df['birthday'] = new_df['birthday'].replace({np.nan: None})
    #new_df['education'] = new_df['education'].replace({np.nan: None})
    #new_df['committees'] = new_df['committees'].replace({np.nan: None})



    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
