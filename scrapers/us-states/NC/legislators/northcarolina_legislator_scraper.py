'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from legislator_scraper_utils import USLegislatorScraperUtils
from bs4 import BeautifulSoup as soup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
import numpy as np
import argparse
import time
import pandas as pd
from urllib.request import urlopen as uReq
import psycopg2
import datefinder
import unidecode
from functools import partial
from database import CursorFromConnectionFromPool
from psycopg2 import sql
from datetime import datetime
import json

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))

scraper_utils = USLegislatorScraperUtils(state_abbreviation, database_table_name, country)


# def get_urls(myurl):
#     '''
#     Insert logic here to get all URLs you will need to scrape from the page.
#     '''

#     # Logic goes here! Some sample code:
# #     base_url = 'https://webscraper.io'
# #     path = '/test-sites/e-commerce/allinone'
# #     scrape_url = base_url + path
# #     page = requests.get(scrape_url)
# #     soup = BeautifulSoup(page.content, 'html.parser')
# #     urls = [base_url + prod_path['href'] for prod_path in soup.findAll('a', {'class': 'title'})]

#     uClient = uReq(myurl)
#     page_html = uClient.read()
#     uClient.close()
#     # # html parsing
#     page_soup = soup(page_html, "html.parser")

#     # #
#     # # #grabs each product
#     reps = page_soup.findAll("div", {"class": "col-8 col-md-7 pr-0"})
#     rep = (reps[0])
#     biographyLinks = []

#     for rep in reps:
#         newurl = rep.a["href"]
#         full = "https://www.ncleg.gov" + newurl
#         biographyLinks.append(full)

#     return biographyLinks


# def scrape(url):
#     '''
#     Insert logic here to scrape all URLs acquired in the get_urls() function.

#     Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
#     state, and state_id values, as these have already been inserted by the initialize_row()
#     function, or will be inserted when placed in the database.

#     Do not worry about trying to insert missing fields as the initialize_row function will
#     insert empty values for us.

#     Be sure to insert the correct data type into each row. Otherwise, you will get an error
#     when inserting data into database. Refer to the data dictionary to see data types for
#     each column.
#     '''

#     row = scraper_utils.initialize_row()

#     # Now you can begin collecting data and fill in the row. The row is a dictionary where the
#     # keys are the columns in the data dictionary. For instance, we can insert the state_url,
#     # like so:
#     row.state_url = url

#     # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
#     # the party name from the website, then get the party_id from scraper_utils
#     # This can be done like so:

#     # Replace with your logic to collect party for legislator.
#     # Must be full party name. Ie: Democrat, Republican, etc.
#     party = 'Republican' 
#     row.party_id = scraper_utils.get_party_id(party) 
#     row.party = party

#     # Other than that, you can replace this statement with the rest of your scraper logic.

#     return row

def isNaN(num):
    return num != num


def collect_legislator_biography_urls(myurl):
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    # #
    # # #grabs each product
    reps = page_soup.findAll("div", {"class": "col-8 col-md-7 pr-0"})
    rep = (reps[0])
    biographyLinks = []

    for rep in reps:
        newurl = rep.a["href"]
        full = "https://www.ncleg.gov" + newurl
        biographyLinks.append(full)

    return biographyLinks


def collect_legislator_committees(biographyUrl):
    myurl = biographyUrl
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    cmtees = page_soup.findAll("div", {"class": "row h-100"})

    cmtee = cmtees[0]

    atag = cmtee.findAll("a", {"class": "nav-item nav-link"})
    acom = atag[2]

    newurl = acom["href"]
    full = "https://www.ncleg.gov/" + newurl

    # read committee page url

    uClient = uReq(full)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    comDiv = page_soup.findAll("div", {"class": "col-9"})
    roleDiv = page_soup.findAll("div", {"class": "col-3 text-right"})
    # list of committees
    committees = []
    if len(comDiv) > 0:
        # if there is at least one committee

        com = comDiv[0]

        # iterate through committees, add them to our list

        for com in comDiv:
            c = com.text
            # get correspoding role for specific committee
            roleIndex = comDiv.index(com)
            role = roleDiv[roleIndex].text

            c = c.strip()
            role = role.strip()

            commiteeUnit = {"role": role, "committee": c}
            committees.append(commiteeUnit)

    return committees


def find_wiki_data(role, repLink):
    try:
        uClient = uReq(repLink)
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
    lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD", "DDS", "MS", "BS", "MBA", "MS", "MD"]

    try:
        uClient = uReq(repLink)
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
        name = name.replace(" (North Carolina politician)", "")


    except:
        name = ""
    name = unidecode.unidecode(name)

    hN = HumanName(name)

    # get occupation
    occupation = []
    if role == "Senator":
        try:
            uClient = uReq(repLink)
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

    info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
            'education': education, 'occupation_wiki': occupation, 'years_active': years_active}

    # print(info)
    return info

    # print(info)
    # print(info)
    # this info will hopefully later be merged with the big legislators info... matching key would be names
    # print(info)

    # except:
    #     print(" ")


def scrape_wiki_bio_Links(wikiUrl, role):
    uClient = uReq(wikiUrl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    # print(page_soup)
    # #
    # # #grabs each product
    repLinks = []
    reps = page_soup.find("table", {"class": "wikitable sortable"})

    reps = reps.tbody
    # print(reps)
    rows = reps.findAll("tr")
    for row in rows:
        try:
            repLink = "https://en.wikipedia.org" + row.a["href"]

            # rep = reps[0]

            # for rep in reps:
            #     try:
            #         repLink = "https://en.wikipedia.org" + rep.a["href"]
            #
            repLinks.append(repLink)
        except:
            repLinks.append("")
    # print(*repLinks, sep="\n")
    return repLinks
    # repLink = repLinks[0]
    # wikiInfosDict = []
    # for repLink in repLinks:
    #     repInfo = find_wiki_data(repLink, role)
    #     wikiInfosDict.append(repInfo)
    #
    # return wikiInfosDict


def collect_legislator_details(biographyUrl):
    myurl = biographyUrl
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    fullname = page_soup.find("div", {"class": "row h-100"})
    htag = fullname.findAll("h1", {"class": "section-title"})
    fulltitle = htag[0].text
    if "Representative" in fulltitle:
        nameAndParty = fulltitle.replace("Representative ", "")
        role = 'Representative'
    elif "Senator" in fulltitle:
        nameAndParty = fulltitle.replace("Senator ", "")
        role = 'Senator'

    # get most_recent_term_id
    seshs = page_soup.findAll("h2", {"class": "card-header"})
    for sesh in seshs:
        if "Session" in sesh.text:
            session = sesh.text.replace(" Session", "")

    distDiv = page_soup.find("div", {
        "class": "col-12 col-sm-7 col-md-8 col-lg-9 col-xl-2 order-2 align-self-center align-self-xl-start mt-3 mt-sm-0"})
    distH = distDiv.find("h6", {"class": "text-nowrap"})
    distText = distH.text

    # regions
    regions = distDiv.findAll("a")
    region = regions[0]
    last = regions[len(regions) - 1]
    areas_served = []
    phones = []
    # phone numbers found on the left side of the page: only on representative's pages
    if last.text.replace("-", "").replace(" ", "").isnumeric():
        # implies that it's a phone number
        phone_number = last.text
        regions.remove(last)
        if role == "Representative":
            office = ""
            phone = {'office': office, 'number': phone_number}
            phones.append(phone)

    else:
        # if no phone number
        phone_number = ""

    # phone numbers found on the right side of the page: Main Phone for reps, Office for Senators
    try:
        rightPhoneDiv = page_soup.find("div", {"class": "col-12 col-md-7 col-lg-9 col-xl-6 text-nowrap"})
        rightPhone = rightPhoneDiv.p.a.text
        rightPhone = rightPhone.replace("(", "")
        rightPhone = rightPhone.replace(")", "")
        if role == "Representative":
            office = "Main"
            p = rightPhone
            phone = {'office': office, 'number': p}
            phones.append(phone)
        elif role == "Senator":
            office = ""
            p = rightPhone
            phone = {'office': office, 'number': p}
            phones.append(phone)
    except:
        pass

    for region in regions:
        areas_served.append(region.text)

    # get occupation, only available for representatives
    if role == "Representative":
        occupation = []
        occ = page_soup.findAll("div", {"class": "col-12 col-md-7 col-lg-9 col-xl-6"})
        if len(occ) > 1:
            occupation.append(occ[1].text)
        else:
            # there is no occupation
            pass


    # get occupation and military service from wikipedia for senators, leave blank in here
    elif role == "Senator":
        occupation = []
        military_experience = ""

    # email
    em = page_soup.findAll("div", {"class": "col-12 col-md-7 col-lg-9 col-xl-6 text-nowrap"})

    try:
        email = em[1].p.a.text
    except:
        # if they don't have an email
        email = ""

    if "(Dem)" in nameAndParty:
        party = "Democrat"
        fullname = nameAndParty.replace("(Dem)", "")
        district = distText.replace("Democrat - District ", "")
    elif "(Rep)" in nameAndParty:
        party = "Republican"
        fullname = nameAndParty.replace("(Rep)", "")
        district = distText.replace("Republican - District ", "")
    else:
        party = "Other/ Independent"
    party_id = scraper_utils.get_party_id(party)
    fullname = unidecode.unidecode(fullname)
    hn = HumanName(fullname)
    name_first = hn.first

    # get military experience if they have it (only available for reps)
    if role == "Representative":
        try:
            milexp = page_soup.find("div", {
                "class": "col-12 col-md-7 col-lg-9 col-xl-6 d-none-text-nowrap d-lg-block-text-nowrap"})
            military_experience = milexp.p.text
        except:
            military_experience = ""

    # addresses
    bio_left_column = page_soup.find("div", {
        "class": "col-12 col-sm-7 col-md-8 col-lg-9 col-xl-2 order-2 align-self-center align-self-xl-start mt-3 mt-sm-0"})
    left_column_tags = bio_left_column.findAll()
    lefttag = left_column_tags[0]
    addresses = []
    for lefttag in left_column_tags:
        if lefttag.text == "Legislative Office:":
            index = left_column_tags.index(lefttag)
            addressText = left_column_tags[index + 1].text + ", " + left_column_tags[index + 2].text
            legOff = {'location': 'Legislative Office', 'address': addressText}
            addresses.append(legOff)

        if lefttag.text == "Mailing Address:":
            index = left_column_tags.index(lefttag)
            addressText = left_column_tags[index + 1].text + ", " + left_column_tags[index + 2].text
            mailAddr = {'location': 'Mailing Address', 'address': addressText}
            addresses.append(mailAddr)

    committees = collect_legislator_committees(biographyUrl)

    legDict = {'state_url': biographyUrl, 'name_full': fullname, 'name_first': name_first, 'name_last': hn.last,
               'name_middle': hn.middle, 'name_suffix': hn.suffix, 'party': party, 'party_id': party_id,
               'district': district,
               'role': role, 'areas_served': areas_served, 'phone_number': phones,
               'occupation': occupation, 'email': email, 'military_experience': military_experience,
               'addresses': addresses, 'committees': committees, 'most_recent_term_id': session}

    # print(legDict)
    return legDict


# if __name__ == '__main__':
#     # First we'll get the URLs we wish to scrape:
#     urls = get_urls()

#     # Next, we'll scrape the data we want to collect from those URLs.
#     # Here we can use Pool from the multiprocessing library to speed things up.
#     # We can also iterate through the URLs individually, which is slower:
#     # data = [scrape(url) for url in urls]
#     with Pool() as pool:
#         data = pool.map(scrape, urls)

#     # Once we collect the data, we'll write it to the database.
#     scraper_utils.insert_legislator_data_into_db(data)

#     print('Complete!')

if __name__ == '__main__':
    # representative data
    bioLinks = collect_legislator_biography_urls('https://www.ncleg.gov/Members/MemberList/H')

    # bL = bioLinks[0]
    # # #
    legislator_data = []

    with Pool() as pool:
        legislator_data = pool.map(func=collect_legislator_details, iterable=bioLinks)

    maindf = pd.DataFrame(legislator_data)

    wikiLinks = scrape_wiki_bio_Links(
        'https://en.wikipedia.org/wiki/North_Carolina_House_of_Representatives',
        "Representative")

    with Pool() as pool:
        role = 'Representative'
        func = partial(find_wiki_data, role)
        wikiData = pool.map(func=func, iterable=wikiLinks)
    wikidf = pd.DataFrame(wikiData)

    mergedRepsData = pd.merge(maindf, wikidf, how='left', on=["name_first", "name_last"])
    # #
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # drop empty occupation column occupation_wiki
    mergedRepsData = mergedRepsData.drop(['occupation_wiki'], axis=1)

    # senator data
    bioLinks = collect_legislator_biography_urls('https://www.ncleg.gov/Members/MemberList/S')
    # get urls for senator's bio pages
    bL = bioLinks[0]
    #
    legislator_data = []

    with Pool() as pool:
        legislator_data = pool.map(func=collect_legislator_details, iterable=bioLinks)
    maindf = pd.DataFrame(legislator_data)

    #
    wikiData = scrape_wiki_bio_Links('https://en.wikipedia.org/wiki/North_Carolina_Senate#Membership', "Senator")

    with Pool() as pool:
        role = 'Senator'
        func = partial(find_wiki_data, role)
        wikiData = pool.map(func=func, iterable=wikiData)
    wikidf = pd.DataFrame(wikiData)

    # merge wiki data and main data

    mergedSensData = pd.merge(maindf, wikidf, how='left', on=["name_first", "name_last"])

    #

    # remove the occupation column which is empty
    mergedSensData = mergedSensData.drop(['occupation'], axis=1)
    mergedSensData = mergedSensData.rename(columns={'occupation_wiki': 'occupation'})
    # rename occupation_wiki column so it has same name as other table

    big_df = (mergedSensData.append(mergedRepsData, sort=True))

    # big_df['party_id'] =

    sample_row = scraper_utils.initialize_row()
    print(sample_row)
    #

    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id
    #
    #
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id

    big_df['state_member_id'] = ""

    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})
    big_df['seniority'] = 0
    print(big_df)
    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')


