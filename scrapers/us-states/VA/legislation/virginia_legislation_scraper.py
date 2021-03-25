import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from multiprocessing import Pool
import pandas as pd
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import urllib.parse as urlparse
from urllib.parse import parse_qs
from pprint import pprint
import datetime
import boto3
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import psycopg2
from nameparser import HumanName
from legislation_scraper_utils import USStateLegislationScraperUtils

import datefinder
import unidecode

# from selenium import webdriver

import PyPDF2
import requests
import io

# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

state_abbreviation = 'VA'
database_table_name = 'us_va_legislation'
legislator_table_name = 'us_va_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

def get_bill_info(myurl):

    committee_info = []

    bill_name = myurl.split("sum+")[1]
    if "S" in bill_name:
        chamber_origin = "Senate"
    if "H" in bill_name:
        chamber_origin = "House"
    if "J" in bill_name:
        bill_type = "Joint Resolution"
    if "R" in bill_name:
        bill_type = "Resolution"
    if "B" in bill_name:
        bill_type = "Bill"

    success = 0
    tries = 0
    while success == 0:
        tries = tries + 1
        try:

            uClient = uReq(myurl)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            topline = page_soup.find("h3")
            bill_title = ""

            bill_title = topline.text

            success = 1
            state_url = myurl
        except:
            myurl2 = myurl.lower().replace('202+sum+', 'ses=202&typ=bil&val=')
            try:
                uClient = uReq(myurl2)
                page_html = uClient.read()
                uClient.close()
                # # html parsing
                page_soup = soup(page_html, "html.parser")

                topline = page_soup.find("h3")
                bill_title = ""

                bill_title = topline.text.strip() + "lower"

                success = 1
                state_url = myurl2
            except:
                pass
        if tries == 5:
            success = 1

    ptext = page_soup.findAll("p")
    bill_description = ""
    bill_summary = ""
    for p in ptext:
        try:
            bill_description = p.b.text
            bill_summary = p.text.replace(bill_description, "")

        except:
            pass

        bill_description = bill_description.replace("\r\n", "")
        bill_summary = bill_summary.replace("\r\n", "")

    linkSections = page_soup.findAll("ul", {"class": "linkSect"})
    pdfsection = linkSections[0]
    pdfli = pdfsection.findAll("a")
    bill_text = ""

    bill_text
    for pl in pdfli:
        if "+pdf" in pl["href"]:
            pdf_link = 'https://lis.virginia.gov/' + pl["href"]
            try:

                r = requests.get(pdf_link)
                f = io.BytesIO(r.content)
                reader = PyPDF2.PdfFileReader(f)

                contents = reader.getPage(0).extractText()
                bill_text = contents

            except:
                pass
    actions = []
    history_section = linkSections[len(linkSections) - 1]

    events = history_section.findAll("li")


    vote_events = []

    for event in events:

        if "House:" in event.text:
            action_by = "House"
            date = event.text.split("House")[0].strip()
            d = datetime.datetime.strptime(date, "%m/%d/%y").strftime("%Y-%m-%d")
            description = event.text.split(":")[1].strip()
            action = {'date': d, 'action_by': action_by, 'description': description}
            actions.append(action)

        elif "Senate:" in event.text:
            action_by = "Senate"
            date = event.text.split("Senate")[0].strip()
            d = datetime.datetime.strptime(date, "%m/%d/%y").strftime("%Y-%m-%d")
            description = event.text.split(":")[1].strip()
            action = {'date': d, 'action_by': action_by, 'description': description}
            actions.append(action)

        if "VOTE" in event.text:

            try:
                vote_link = 'https://lis.virginia.gov/' + (event.a["href"])
                uClient = uReq(vote_link)
                page_html = uClient.read()
                uClient.close()
                # # html parsing
                vote_soup = soup(page_html, "html.parser")

                all_p = vote_soup.findAll("p")

                date = d
                vote_description = event.text.split("VOTE:")[1]

                vote_description = vote_description.split("(")[0].strip()

                person_votes = []
                nv = 0
                absent = 0
                for p in all_p:

                    if "YEAS" in p.text:
                        yea_people = []
                        if len(p.text.split('--')) == 3:

                            yeas = p.text.split("--")[2]
                            yeas = yeas.replace(".", "").strip()
                            yeas = int(yeas)

                            yea_people = p.text.split("--")[1]
                            yea_people = yea_people.split(",")
                            # print(yea_people)

                            for name in yea_people:
                                if name.strip() != "" and 'Speaker' not in name:
                                    legislator = name.strip()
                                    # leg_name = HumanName(legislator)
                                    # legislator = leg_name.last
                                    vote = "yea"
                                    gov_id = ""
                                    search_for = dict(name_last=legislator)
                                    try:
                                        gov_id = scraper_utils.get_legislator_id(**search_for)
                                    except:
                                        pass
                                    v = {"legislator": legislator, "goverlytics_id": gov_id, "vote": vote}
                                    person_votes.append(v)

                        elif len(p.text.split('--')) == 2:
                            yeas = p.text.split("--")[1]
                            yeas = yeas.replace(".", "").strip()
                            yeas = int(yeas)

                    if "NAYS" in p.text:
                        nay_people = []
                        if len(p.text.split('--')) == 3:
                            nays = p.text.split("--")[2]
                            nays = nays.replace(".", "").strip()
                            nays = int(nays)

                            nay_people = p.text.split("--")[1]
                            nay_people = nay_people.split(",")
                            if len(nay_people) != 0:

                                for name in nay_people:
                                    if name.strip() != "" and 'Speaker' not in name:
                                        legislator = name.strip()

                                        vote = "nay"
                                        gov_id = ""
                                        search_for = dict(name_last=legislator)
                                        try:
                                            gov_id = scraper_utils.get_legislator_id(**search_for)
                                        except:
                                            pass
                                        v = {"legislator": legislator, "goverlytics_id": gov_id, "vote": vote}
                                        person_votes.append(v)
                        elif len(p.text.split('--')) == 2:
                            nays = p.text.split("--")[1]
                            nays = nays.replace(".", "").strip()
                            nays = int(nays)



                        if yeas > nays:
                            passed = 1
                        else:
                            passed = 0

                    if "ABSTENTIONS" in p.text:
                        absent_people = []
                        if len(p.text.split('--')) == 3:
                            absent = p.text.split("--")[2]
                            absent = absent.replace(".", "").strip()
                            absent = int(absent)

                            absent_people = p.text.split("--")[1]
                            absent_people = absent_people.split(",")
                        elif len(p.text.split('--')) == 2:
                            absent = p.text.split("--")[1]
                            absent = absent.replace(".", "").strip()
                            absent = int(absent)


                    if "NOT VOTING" in p.text:
                        nv = p.text.split("--")[2]
                        nv = nv.replace(".", "").strip()
                        nv = int(nv)

                        nv_people = p.text.split("--")[1]
                        nv_people = nv_people.split(",")


                votes = {'date': date, 'description': vote_description, 'yea': yeas, 'nays': nays, 'nv': nv,
                         'absent': absent, 'total': nays + yeas, 'passed': passed, 'chamber': action_by,
                         'votes': person_votes}


                vote_events.append(votes)


            except Exception as ex:

                template = "An exception of type {0} occurred. Arguments:\n{1!r}"

                message = template.format(type(ex).__name__, ex.args)

                # print(message)
    # print(vote_events)
    first_action = actions[0]
    date_introduced = first_action["date"]

    actions.reverse()
    current_status = actions[0]["description"]
    committees = []
    for action in actions:
        if "Committee" in action["description"]:
            if "Referred to Committee" in action["description"]:
                com = (action["description"]).split("Referred to ")[1]
                chamber = action["action_by"]
                committee = {'chamber': chamber, 'committee': com}
                committees.append(committee)

            else:
                com = (action["description"].split("Committee")[0]).strip()
                if com != "":
                    chamber = action["action_by"]
                    committee = {'chamber': chamber, 'committee': com}
                    committees.append(committee)

    session_info = page_soup.find("h2")
    session = session_info.text

    marg_section = page_soup.find("p", {"class": "sectMarg"})
    principal_sponsor = marg_section.a.text
    pshn = HumanName(principal_sponsor)
    principal_sponsor_id = 0
    principal_sponsor = pshn.last
    search_for = dict(name_last=pshn.last, name_first=pshn.first)

    principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)


    all_patrons = marg_section.findAll("a")[1]
    patrons_link = 'https://lis.virginia.gov/' + all_patrons["href"]

    sponsors = []
    sponsors_id = []
    try:
        uClient = uReq(patrons_link)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        patron_soup = soup(page_html, "html.parser")
        linkSect = patron_soup.find("ul", {"class": "linkSect"})

        lis = linkSect.findAll("li")
        for li in lis:
            patron = (li.a.text).split("(")[0].strip()
            pshn = HumanName(patron)

            patron = pshn.last
            search_for = dict(name_last=pshn.last, name_first=pshn.first)

            sponsor_id = scraper_utils.get_legislator_id(**search_for)
            sponsors.append(patron)
            sponsors_id.append(sponsor_id)
    except:
        sponsors = [principal_sponsor]
        sponsors_id = [principal_sponsor_id]

    # if pshn.first in dbwork.psfirstnames and pshn.last in dbwork.pslastnames:
    #     suindex = dbwork.psfirstnames.index(pshn.first)
    #     if dbwork.psids[suindex] not in sponsors_id:
    #         principal_sponsor_id = dbwork.psids[suindex]
    #         sponsors_id.append(dbwork.psids[suindex])


    # get goverlytics_id, url
    goverlytics_id = "VA_2019-2020_" + bill_name
    url = "/us/VA/legislation/" + goverlytics_id

    bill_info = {'state_url': state_url, 'bill_name': bill_name, 'bill_type': bill_type,
                 'chamber_origin': chamber_origin, 'state': 'VA', 'state_id': 51, 'bill_state_id': "",
                 'bill_title': bill_title,
                 'bill_description': bill_description.strip(), 'bill_summary': bill_summary.strip(),
                 'bill_text': bill_text, 'actions': actions, 'date_introduced': date_introduced,
                 'current_status': current_status, 'session': session, 'principal_sponsor': principal_sponsor,
                 'principal_sponsor_id': principal_sponsor_id, 'sponsors': sponsors, 'sponsors_id': sponsors_id,
                 'cosponsors': [], 'cosponsors_id': [], 'goverlytics_id': goverlytics_id, 'url': url,
                 'committees': committees, 'site_topic': "", 'votes': vote_events, 'topic': "",
                 'country_id': scraper_utils.country_id, 'country': scraper_utils.country}
    # print(bill_info)
    return bill_info


if __name__ == '__main__':
    #
    bill_infos = []

    failed = 0
    i = 1735
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?211+sum+HB' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1
    #
    failed = 0
    i = 270
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1


    failed = 0
    i = 272
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1


    failed = 0
    i = 275
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    #
    failed = 0
    i = 285
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"

            message = template.format(type(ex).__name__, ex.args)

            # print(message)
            failed = 1


    failed = 0
    i = 288
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 292
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1
    #
    failed = 0
    i = 308
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 310
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1


    failed = 0
    i = 322
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1



    failed = 0
    i = 395
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 5001
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SJ' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 501
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+HR' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            failed = 1

    failed = 0
    i = 1097
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SB' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            if i > 1476:
                failed = 1

    failed = 0
    i = 501
    while failed == 0:
        bill_link = 'https://lis.virginia.gov/cgi-bin/legp604.exe?212+sum+SR' + str(i)
        try:
            bill_info = get_bill_info(bill_link)
            if bill_info not in bill_infos:
                bill_infos.append(bill_info)
            i += 1
        except:
            if i > 560:
                failed = 1






    # with Pool() as pool:
    #     # #
    #     bill_data = pool.map(func=app.get_bill_info, iterable=all_links)

    big_df = pd.DataFrame(bill_infos)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    big_df['source_url'] = big_df['state_url']
    big_df['source_id'] = big_df['bill_state_id']

    # big_df = topics.add_topics(bill_df)
    print(big_df)

    print(big_df)
    big_list_of_dicts = big_df.to_dict('records')
    print(big_list_of_dicts)

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')
