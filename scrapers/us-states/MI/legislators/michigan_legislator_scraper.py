import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
from legislator_scraper_utils import USStateLegislatorScraperUtils

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
from geotext import GeoText
# import html.parser
import requests
from requests import get

scraper_utils = USStateLegislatorScraperUtils('MI', 'us_mi_legislators')


def decodeEmail(e):
    de = ""
    k = int(e[:2], 16)

    for i in range(2, len(e) - 1, 2):
        de += chr(int(e[i:i + 2], 16) ^ k)

    return de


def get_sen_bio(myurl):
    links = []
    uClient = uReq(myurl)
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
        phone_number = []
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
                phone_number.append(pn)

        # get addresses
        addresses = []
        alltags = si.findAll()
        lasttag = alltags[len(alltags) - 1]

        address_info = lasttag.nextSibling.replace("Office: ", "")
        address_info = address_info.strip()
        location = "office"
        addr = {'location': location, "address": address_info}
        addresses.append(addr)

        sd = {'source_url': str(url).strip(), 'name_full': str(hN), 'name_first': hN.first, 'name_last': hN.last,
              'name_middle': hN.middle, 'name_suffix': hN.suffix, 'party': party, 'party_id': party_id,
              'district': district, 'role': 'Senator', 'phone_number': phone_number, 'addresses': addresses}
        print(sd)
        senator_data.append(sd)

    return senator_data


def get_rep_bio(myurl):
    rep_bios = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tbl = page_soup.find("table", {"id": "grvRepInfo"})
    links = tbl.findAll("td")
    i = 1
    for link in links:
        if i % 7 == 1:
            # website
            state_url = link.a["href"]
            # if "housedems" not in state_url:
            #     state_url =state_url + "about"
            # else:
            #     state_url = state_url + "about/"
            # print(state_url)

        if i % 7 == 2:
            # district
            district = link.text

        if i % 7 == 3:
            # name
            name_full = link.text

            hn = HumanName(name_full)
        if i % 7 == 4:
            # party
            party = ""
            p = link.text
            if p == "R":
                party = "Republican"
                party_id = 3
            if p == "D":
                party = "Democrat"
                party_id = 2

        if i % 7 == 6:
            # phone
            phone = link.text
            phone = phone.replace("517373", "517-373-")

            phone_number = {"office": '', 'number': phone}
            # print(phone_number)

        if i % 7 == 0:
            # email
            email = link.a["href"]
            bio_info = {'source_url': state_url, 'district': district, 'name_full': name_full, 'name_first': hn.first,
                        'name_last': hn.last, 'name_middle': hn.middle, 'name_suffix': hn.suffix, 'party': party,
                        'party_id': party_id, 'phone_number': phone_number, 'email': email.replace("mailto:", ""),
                        'role': 'Representative',
                        'state': 'MI', 'state_id': 26, 'country': 'United States of America', 'country_id': 1}
            print(bio_info)
            if name_full != "Vacant":
                rep_bios.append(bio_info)

        i = i + 1
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

            uReq(req).close()

            page_soup = soup(webpage, "html.parser")

            bio_paragraph = page_soup.find("div", {"class": "elementor-text-editor elementor-clearfix"})
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

            contact = page_soup.find("div", {"class": "font-condensed text-15"})
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

                        address_info = {'address': addy.replace("\n", ", "), 'location': location}

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
        get_in_touch = page_soup.find("div", {"class": "fusion-text fusion-text-3"})
        lis = get_in_touch.findAll("li")
        address = lis[2].text.strip()

        addr_info = {'address': address.replace("\xa0", ""), 'location': 'office'}
        addresses.append(addr_info)

    rep_info = {'source_url': myurl, 'addresses': addresses}
    return rep_info


def get_house_committee_info(myurl):
    committee_info = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing

    page_soup = soup(page_html, "html.parser")
    tdcomm = page_soup.findAll("td")
    # commnames = page_soup.findAll("div", {"class": "DataListCommName"})
    for td in tdcomm:
        com_name = td.div.text

        participants = td.find("div", {"class": "DataMemberName"})
        people = (participants.text)
        participantlist = people.split(",")
        for pl in participantlist:
            role = ""
            name_role = pl.split(" (")
            name = name_role[0]
            hn = HumanName(name)
            name_last = hn.last
            name_first = hn.first

            # print(name_last)
            try:
                r = name_role[1]
                if "C)" in r and "VC" not in r:
                    role = "Chair"
                else:
                    role = r.replace(")", "")

            except:
                pass
            role_and_committee = {"role": role, 'committee': com_name}
            # role_and_committee = empty_list.append(role_and_committee)
            comm_person = {'name_last': name_last, 'name_first': name_first, 'committees': role_and_committee}
            committee_info.append(comm_person)

    return committee_info


def get_rep_wiki_links(repLink):
    wiki_info = []
    uClient = uReq(repLink)
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
                wi = {'wiki_url': repLink, 'areas_served': areas_served}
                wiki_info.append(wi)
                # print(wi)

        except:
            pass

    return wiki_info


# def find_wiki_data(repLink):
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
#     # get years_active, based off of "assumed office"
#     years_active = []
#     year_started = ""
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         table = page_soup.find("table", {"class": "infobox vcard"})
#
#         tds = table.findAll("td", {"colspan": "2"})
#         td = tds[0]
#
#         for td in tds:
#             asof = (td.find("span", {"class": "nowrap"}))
#             if asof != None:
#                 if (asof.b.text) == "Assumed office":
#
#                     asofbr = td.find("br")
#
#                     year_started = (asofbr.nextSibling)
#
#                     year_started = year_started.split('[')[0]
#                     if "," in year_started:
#                         year_started = year_started.split(',')[1]
#                     year_started = (year_started.replace(" ", ""))
#                     year_started = re.sub('[^0-9]', '', year_started)
#                     if year_started.startswith("12"):
#                         year_started = year_started.substring(1)
#
#
#
#                 else:
#                     pass
#
#     except Exception as ex:
#
#         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         # print(message)
#
#     if year_started != "":
#         years_active = list(range(int(year_started), 2021))
#         # years_active_lst.append(years_active_i)
#     else:
#         years_active = []
#         # years_active_i = []
#         # years_active_i.append(years_active)
#         # years_active_lst.append(years_active_i)
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
#     # get occupation
#     occupation = []
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
#
#         left_column_tags = reps.findAll()
#         lefttag = left_column_tags[0]
#         for lefttag in left_column_tags:
#             if lefttag.text == "Occupation":
#                 index = left_column_tags.index(lefttag) + 1
#                 occ = left_column_tags[index].text
#                 if occ != "Occupation":
#                     occupation.append(occ)
#
#     except:
#         pass
#
#     try:
#         mrtid = str(years_active[len(years_active) - 1])
#     except:
#         mrtid = '2021'
#
#     info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
#             'education': education, 'occupation': occupation, 'years_active_wiki': years_active,
#             'most_recent_term_id': mrtid}
#
#     # print(info)
#     return info


# def find_wiki_rep_data(dict):
#     areas_served = dict['areas_served']
#     repLink = dict['wiki_url']
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
#         # get years_active, based off of "assumed office"
#     years_active = []
#     year_started = ""
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         table = page_soup.find("table", {"class": "infobox vcard"})
#
#         tds = table.findAll("td", {"colspan": "2"})
#         td = tds[0]
#
#         for td in tds:
#             asof = (td.find("span", {"class": "nowrap"}))
#             if asof != None:
#                 if (asof.b.text) == "Assumed office":
#
#                     asofbr = td.find("br")
#
#                     year_started = (asofbr.nextSibling)
#
#                     year_started = year_started.split('[')[0]
#                     if "," in year_started:
#                         year_started = year_started.split(',')[1]
#                     year_started = (year_started.replace(" ", ""))
#                     year_started = re.sub('[^0-9]', '', year_started)
#                     if year_started.startswith("12"):
#                         year_started = year_started.substring(1)
#
#
#
#                 else:
#                     pass
#
#     except Exception as ex:
#
#         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         # print(message)
#
#     if year_started != "":
#         years_active = list(range(int(year_started), 2021))
#         # years_active_lst.append(years_active_i)
#     else:
#         years_active = []
#         # years_active_i = []
#         # years_active_i.append(years_active)
#         # years_active_lst.append(years_active_i)
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
#         name = name.replace(" (Michigan politician)", "")
#
#
#     except:
#         name = ""
#     name = unidecode.unidecode(name)
#
#     hN = HumanName(name)
#
#     # get occupation
#     occupation = []
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
#
#         left_column_tags = reps.findAll()
#         lefttag = left_column_tags[0]
#         for lefttag in left_column_tags:
#             if lefttag.text == "Occupation":
#                 index = left_column_tags.index(lefttag) + 1
#                 occ = left_column_tags[index].text
#                 if occ != "Occupation":
#                     occupation.append(occ)
#
#     except:
#         pass
#     most_recent_term_id = ""
#     try:
#         most_recent_term_id = str(years_active[len(years_active) - 1])
#     except:
#         pass
#
#     info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
#             'education': education, 'occupation': occupation, 'years_active': years_active,
#             'most_recent_term_id': most_recent_term_id, 'areas_served': areas_served, 'seniority': "",
#             'military_experience': ""}
#
#     # print(info)
#     return info


def scrape_wiki_bio_Links(wikiUrl):
    repLinks = []
    uClient = uReq(wikiUrl)
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
            comm_person = {'name_last': name_last, 'committees': role_and_committee}
            committee_info.append(comm_person)
    except:
        pass

    print(committee_info)

    return committee_info

def wiki_rep_areas(area_link_dict):
    wiki_dict = scraper_utils.scrape_wiki_bio(area_link_dict['wiki_url'])
    wiki_dict['areas_served'] = area_link_dict['areas_served']
    wiki_dict['seniority'] = 0
    wiki_dict['military_experience'] = ""
    return wiki_dict


if __name__ == '__main__':
    sen_page = 'https://senate.michigan.gov/senatorinfo_complete.html'
    house_page = 'https://www.house.mi.gov/MHRPublic/frmRepListMilenia.aspx?all=true'
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # get links for each person's page
    sen_bio = get_sen_bio(sen_page)
    bio_df = pd.DataFrame(sen_bio)
    sen_links = [(d['source_url']) for d in sen_bio]
    sen_party = [(d['party']) for d in sen_bio]
    # print(sen_links)
    # print(len(sen_links))

    # print(sen_links)

    with Pool() as pool:
        sen_data = pool.map(func=collect_sen_data, iterable=sen_links)
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
    print(committees)

    with Pool() as pool:

        commData = pool.map(func=get_committee_info, iterable=committees)
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
    wiki_senate_link = 'https://en.wikipedia.org/wiki/Michigan_Senate'
    wikiData = scrape_wiki_bio_Links(wiki_senate_link)

    with Pool() as pool:

        wikiData = pool.map(func=scraper_utils.scrape_wiki_bio, iterable=wikiData)
    wikidf = pd.DataFrame(wikiData)
    print("pool 3")
    # print("wikidf")

    wikidf = wikidf.rename(columns={'years_active': 'years_active_wiki'})

    mergedSensData = pd.merge(sendf, wikidf, how='left', on=["name_first", "name_last"])
    print(mergedSensData)

    mergedSensData['years_active'] = np.where(mergedSensData['years_active'] == 0, mergedSensData['years_active_wiki'],
                                              mergedSensData['years_active'])

    mergedSensData = mergedSensData.drop(labels="years_active_wiki", axis=1)
    mergedSensData['birthday'] = mergedSensData['birthday'].replace({np.nan: None})
    mergedSensData['occupation'] = mergedSensData['occupation'].replace({np.nan: None})
    mergedSensData['most_recent_term_id'] = mergedSensData['most_recent_term_id'].replace({np.nan: None})
    mergedSensData['years_active'] = mergedSensData['years_active'].replace({np.nan: None})
    mergedSensData['education'] = mergedSensData['education'].replace({np.nan: None})
    print(mergedSensData)

    rep_bio = get_rep_bio(house_page)
    rep_bio_df = pd.DataFrame(rep_bio)
    # print(rep_bio)
    rep_links = [(d['source_url']) for d in rep_bio]
    rep_party = [(d['party']) for d in rep_bio]

    with Pool() as pool:
        rep_data = pool.map(func=collect_rep_data, iterable=rep_links)
    rep_df = pd.DataFrame(rep_data)
    # print(rep_df)
    rep_df = pd.merge(rep_df, rep_bio_df, how='left', on=['source_url'])

    comm_link = 'https://www.house.mi.gov/MHRPublic/standingcommittee.aspx'
    commData = get_house_committee_info(comm_link)
    comm_list = []

    for item in commData:
        contained = 0
        for dict in comm_list:

            if dict["name_last"] == item["name_last"] and dict["name_first"] == item["name_first"]:
                # print(dict["committees"])

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
    # print(comm_df)
    #
    rep_df = pd.merge(rep_df, comm_df, how='left', on=["name_last", "name_first"])

    rep_wiki_link = 'https://en.wikipedia.org/wiki/Michigan_House_of_Representatives'
    wiki_links_areas = get_rep_wiki_links(rep_wiki_link)
    print(wiki_links_areas)
    # wla_rep_df = pd.DataFrame(wiki_links_areas)
    # wiki_links = wla_rep_df['wiki_url']
    # wla_rep_df['seniority'] = 0
    # wla_rep_df['military_experience'] = ""


    with Pool() as pool:
        rep_data = pool.map(func=wiki_rep_areas, iterable=wiki_links_areas)
    wiki_df = pd.DataFrame(rep_data)
    print(wiki_df)


    mergedRepsData = pd.merge(rep_df, wiki_df, how='left', on=["name_first", "name_last"])
    mergedRepsData['committees'] = mergedRepsData['committees'].replace({np.nan: None})
    mergedRepsData['seniority'] = mergedRepsData['seniority'].replace({np.nan: None})
    mergedRepsData['military_experience'] = mergedRepsData['military_experience'].replace({np.nan: None})
    mergedRepsData['areas_served'] = mergedRepsData['areas_served'].replace({np.nan: None})
    mergedRepsData['most_recent_term_id'] = mergedRepsData['most_recent_term_id'].replace({np.nan: None})
    mergedRepsData['years_active'] = mergedRepsData['years_active'].replace({np.nan: None})
    mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({np.nan: None})
    mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({np.nan: None})
    mergedRepsData['education'] = mergedRepsData['education'].replace({np.nan: None})
    mergedRepsData['committees'] = mergedRepsData['committees'].replace({np.nan: None})

    mergedRepsData["source_id"] = ""


    print(mergedRepsData)

    big_df = (mergedSensData.append(mergedRepsData, sort=True))
    big_df['seniority'] = None
    sample_row = scraper_utils.initialize_row()
    # print(sample_row)
    #

    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id


    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id

    print(big_df)
