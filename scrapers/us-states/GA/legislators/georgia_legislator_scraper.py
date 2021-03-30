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

from legislator_scraper_utils import USStateLegislatorScraperUtils
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
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')

driver = webdriver.Chrome('../../../../web_drivers/chrome_win_89.0.4389.23/chromedriver.exe',
                          chrome_options=chrome_options)


def collect_rep_bio_info(myurl):
    links = []
    # uClient = uReq(myurl)
    # page_html = uClient.read()
    # uClient.close()
    # # # html parsing
    # page_soup = soup(page_html, "html.parser")




    try:

        driver.get(myurl)
        timeout = 5

        element_present = EC.presence_of_element_located((By.CLASS_NAME, 'table table-striped sortable memberList'))
        WebDriverWait(driver, timeout).until(element_present)


    except:
        pass

    html = driver.page_source
    page_soup = soup(html, 'html.parser')

    public_info = page_soup.findAll("a", {"class": "btn responseBtn btn-sm btn-outline-primary"})
    csv_link = (public_info[1]["href"])

    test_df = pd.read_excel(csv_link)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    test_df = test_df.rename(columns={'last name': 'name_last', 'first name': 'name_first',
                                      'counties represented': 'areas_served', 'dob': 'birthday',
                                      'legislative email': 'email', 'Committee #1': 'com1', 'Committee #2': 'com2',
                                      'Committee #3': 'com3', 'Committee #4': 'com4', 'Committee #5': 'com5',
                                      'Committee #6': 'com6', 'Committee #7': 'com7', 'Committee #8': 'com8',
                                      'Committee #9': 'com9', 'Committee #10': 'com10', 'Committee #11': 'com11',
                                      'street address': 'streetaddress', 'street address 2': 'streetaddress2',
                                      'house office': 'houseoffice', 'capitol zip': 'capitolzip',
                                      'capitol address': 'capitoladdress'})
    test_df['name_full'] = test_df['name_first'] + ' ' + test_df['name_last']

    def make_array(words):
        return_arr = []
        try:
            arr = words.split(",")

            for a in arr:
                return_arr.append(a.strip())
        except:
            pass
        return return_arr

    test_df['areas_served'] = test_df['areas_served'].apply(make_array)
    test_df['occupation'] = test_df['occupation'].apply(make_array)

    def party_full_name(p):
        if p == 'D':
            party = 'Democrat'
        elif p == 'R':
            party = 'Republican'
        else:
            party = 'Other'
            print(p)
        return party

    # use scraper utils to get party ids from db
    test_df['party'] = test_df['party'].apply(party_full_name)
    # test_df['party_id'] = test_df['party'].apply(get_party_id)

    test_df['name_middle'] = ""
    test_df['name_suffix'] = ""
    test_df['role'] = "Representative"
    test_df['seniority'] = 0
    test_df['military_experience'] = ""

    def get_years_active(sid):
        sid = str(sid)
        year_started = sid.split("-")[0]
        years_active = []
        if year_started != "":
            years_active = list(range(int(year_started), 2021))
            # years_active_lst.append(years_active_i)
        else:
            years_active = []
        return years_active

    test_df['years_active'] = test_df['swearing in date'].apply(get_years_active)
    test_df['most_recent_term_id'] = '2021-2022'

    def to_date(birth_time):
        return birth_time.to_pydatetime()

    test_df['birthday'] = test_df['birthday'].apply(to_date)

    def add_district_phone(number):
        phone_number = []
        try:
            number = number.replace(".", "-")
            phone_number.append({'office': 'district office', 'number': number})
        except:
            pass
        return phone_number

    def add_house_phone(number):
        phone_number = []
        try:
            number = number.replace(".", "-")
            phone_number.append({'office': 'house', 'number': number})
        except:
            pass
        return phone_number

    test_df['districtphone'] = test_df['district phone'].apply(add_district_phone)
    test_df['housephone'] = test_df['house phone'].apply(add_house_phone)

    def append_phones(dp, hp):
        return dp + hp

    def append_committees(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11):
        committees = []
        com_list = [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11]
        for c in com_list:
            try:
                if c:
                    item = {'role': '', 'committee': c.strip()}
                    committees.append(item)
            except:
                pass

        return committees

    def append_addresses(streetaddress, streetaddress2, city, state, zip, houseoffice, capitoladdress, capitolzip):
        street_address = ""
        house_address = ""
        addresses = []
        try:
            street_address = street_address + streetaddress.strip() + ", "
        except:
            pass
        try:
            street_address = street_address + streetaddress2.strip()  + ", "
        except:
            pass
        try:
            street_address = street_address + city.strip() + ", "
        except:
            pass
        if street_address != "":
            try:
                street_address = street_address + state.strip()  + ", "
            except:
                pass
            try:
                street_address = street_address + str(zip).strip()
            except:
                pass
            street = {'location': 'street address', 'address': street_address}
            addresses.append(street)

        try:
            house_address = house_address + houseoffice.strip() + ", "
        except:
            pass
        try:
            house_address = house_address + capitoladdress.strip() + ", "
        except:
            pass
        if house_address != "":
            try:
                house_address = house_address + str(capitolzip).strip()
            except:
                pass
            capitol = {'location': 'capitol address/ house office', 'address': house_address}
            addresses.append(capitol)



        return addresses

    test_df['phone_number'] = test_df.apply(lambda row: append_phones(row.districtphone, row.housephone), axis=1)

    test_df['committees'] = test_df.apply(lambda row: append_committees(row.com1, row.com2, row.com3, row.com4,
                                                                        row.com5, row.com6, row.com7, row.com8,
                                                                        row.com9, row.com10, row.com11), axis=1)

    test_df['addresses'] = test_df.apply(lambda row: append_addresses(row.streetaddress, row.streetaddress2,
                                                                      row.city, row.state, row.zip, row.houseoffice,
                                                            row.capitoladdress, row.capitolzip), axis=1)
    test_df['state_url'] = ''
    test_df['state_member_id'] = ''
    test_df['party_id'] = test_df.apply(lambda row: scraper_utils.get_party_id(row.party), axis=1)



    return test_df



def scrape_wiki_rep_Links(wikiUrl):
    repLinks = []
    uClient = uReq(wikiUrl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")


    table = page_soup.find("table", {'class': 'sortable wikitable'})
    tds = table.findAll("td")
    i = 0
    for td in tds:
        i = i + 1
        try:
            if i%6 == 2:
                link = 'https://en.wikipedia.org/' + (td.a['href'])
                repLinks.append(link)
        except:
            pass
    # print(tables)
    # rep = reps[0]
    # repLinks = []
    # for rep in reps:
    #     try:
    #         repLink = "https://en.wikipedia.org" + rep.a["href"]
    #
    #         repLinks.append(repLink)
    #     except:
    #         repLinks.append("")
    # print(repLinks)

    return repLinks

    # titlemain = page_soup.find("main", {"class": "col-12 body-content ncga-container-gutters"})


def find_wiki_data(repLink):
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        # reps = page_soup.find("div", {"class": "mw-parser-output"})
        # repBirth = reps.find("span", {"class": "bday"}).text

        lefttags = page_soup.findAll()

        for lt in lefttags:
            if lt.text == "Born":
                ltindex = lefttags.index(lt)
                sp = lefttags[ltindex + 1]

                repBirth = sp.span.text
                repBirth = repBirth.replace("(", "")
                repBirth = repBirth.replace(")", "")
                repBirth = repBirth.replace(" ", "")

        b = datetime.datetime.strptime(repBirth, "%Y-%m-%d")

        birthday = b.date()


    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)

        # print(message)
        # couldn't find birthday in side box
        birthday = None

        # get years_active, based off of "assumed office"
    # years_active = []
    # year_started = ""
    # try:
    #     uClient = uReq(repLink)
    #     page_html = uClient.read()
    #     uClient.close()
    #     # # html parsing
    #     page_soup = soup(page_html, "html.parser")
    #
    #     table = page_soup.find("table", {"class": "infobox vcard"})
    #
    #     tds = table.findAll("td", {"colspan": "2"})
    #     td = tds[0]
    #
    #     for td in tds:
    #         asof = (td.find("span", {"class": "nowrap"}))
    #         if asof != None:
    #             if (asof.b.text) == "Assumed office":
    #
    #                 asofbr = td.find("br")
    #
    #                 year_started = (asofbr.nextSibling)
    #
    #                 year_started = year_started.split('[')[0]
    #                 if "," in year_started:
    #                     year_started = year_started.split(',')[1]
    #                 year_started = (year_started.replace(" ", ""))
    #                 year_started = re.sub('[^0-9]', '', year_started)
    #                 if year_started.startswith("12"):
    #                     year_started = year_started.substring(1)
    #
    #
    #
    #             else:
    #                 pass
    #
    # except Exception as ex:
    #
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     # print(message)
    #
    # if year_started != "":
    #     years_active = list(range(int(year_started), 2021))
    #     # years_active_lst.append(years_active_i)
    # else:
    #     years_active = []
    #     # print("empty")
    #     # years_active_i = []
    #     # years_active_i.append(years_active)
    #     # years_active_lst.append(years_active_i)

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
        name = name.replace(" (Georgia politician)", "")


    except:
        name = ""
    name = unidecode.unidecode(name)

    hN = HumanName(name)

    info = {'name_first': hN.first, 'name_last': hN.last,
            'education': education}

    # print(info)

    return info


if __name__ == '__main__':
    # representative data
    repbioInfo = collect_rep_bio_info('https://www.legis.ga.gov/members/house')
    repdf = repbioInfo

    wikiLinks = scrape_wiki_rep_Links(
        'https://en.wikipedia.org/wiki/Georgia_House_of_Representatives')
    with Pool() as pool:
        wikiData = pool.map(func=find_wiki_data, iterable=wikiLinks)
    wikidfreps = pd.DataFrame(wikiData)
    #
    mergedRepsData = pd.merge(repdf, wikidfreps, how='left', on=["name_first", "name_last"])
    #


    big_df = mergedRepsData
    sample_row = scraper_utils.initialize_row()

    #

    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id
    #
    #
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['occupation'] = big_df['occupation'].replace({np.nan: None})
    big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
    big_df['education'] = big_df['education'].replace({np.nan: None})

    big_df = big_df.drop(labels=['streetaddress', 'streetaddress2', 'city', 'zip', 'district phone', 'houseoffice',
                        'capitoladdress', 'capitolzip', 'house phone', 'house staff', 'spouse name', 'desk',
                        'page location', 'swearing in date', 'title', 'com1', 'com2', 'com3', 'com4', 'com5', 'com6',
                        'com7', 'com8', 'com9', 'com10', 'com11', 'districtphone', 'housephone'], axis=1)
    print(big_df)
    big_list_of_dicts = big_df.to_dict('records')
    print(big_list_of_dicts)


    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

