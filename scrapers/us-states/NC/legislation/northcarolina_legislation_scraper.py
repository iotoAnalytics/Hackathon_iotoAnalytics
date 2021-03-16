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

# packages required for topic classifier
import nltk

from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from nltk.stem import WordNetLemmatizer
from joblib import dump, load
from sklearn import linear_model

# # Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')

state_abbreviation = 'NC'
database_table_name = 'us_nc_legislation'
legislator_table_name = 'us_nc_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)


def collect_bill_urls(myurl):
    link = ""
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    bill_infos = []
    # #
    # # #grabs each product
    billTable = page_soup.find("table", {"id": "bill-report"})
    billTbody = billTable.find("tbody")
    billsa = billTbody.findAll("a")

    billa = billsa[0]

    for billa in billsa:
        link = billa["href"]
        bill_name = link.replace("/BillLookUp/2021/", "")

        link = "https://www.ncleg.gov" + link
        gd = ("NC_20202021_" + bill_name)
        url = '/us/nc/legislation/' + gd
        bill_info = {'source_url': link, 'bill_name': bill_name, 'goverlytics_id': gd, 'url': url}
        if bill_info not in bill_infos:
            bill_infos.append(bill_info)
    return bill_infos


def collect_vote_info(link):
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    # get date, outcome
    vd = page_soup.find("div", {"class": "card ncga-card-top-border mb-5"})
    col = vd.findAll("div", {"class": "col-12"})
    date = (col[4].text).replace("Time: ", "")
    outcome = (col[2].text).replace("Outcome: ", "")
    # could do passed as a binary value
    if outcome == "PASS":
        passed = 1
    else:
        passed = 0

    # get chamber
    chamber = ""
    sectiontitle = page_soup.find("div", {"class": "section-title"})

    if "House" in sectiontitle.text:
        chamber = "House"
    if "Senate" in sectiontitle.text:
        chamber = "Senate"

    # get description
    de = page_soup.find("div", {"class": "col-12 col-lg-4"})
    description = de.text
    nonBreakSpace = u'\xa0'
    description = description.replace(nonBreakSpace, '')
    description = description.replace('Vote:', "")
    # get yea, nay, total, nv, absent #
    divs = page_soup.findAll("div", {"class": "col-12 col-md-6"})
    div = divs[0]
    total = ""
    yea = ""
    nay = ""
    nv = ""
    absent = ""

    for div in divs:
        if div.span.text == "Ayes:":
            yea = div.text.replace("Ayes: ", "")

        if div.span.text == "Noes:":
            nay = div.text.replace("Noes: ", "")

        if div.span.text == "Total votes:":
            total = div.text.replace("Total votes: ", "")

        if div.span.text == "Not Voting:":
            nv = div.text.replace("Not Voting: ", "")

        if div.span.text == "Excused Absence:":
            absent = div.text.replace("Excused Absence: ", "")

    # get votes
    votes = []
    ayes = ""
    mt = page_soup.findAll("div", {"class": "row ncga-row-no-gutters mt-3"})
    ayesList = mt[0].findAll("div", {"class": "col-12"})
    noesList = mt[1].findAll("div", {"class": "col-12"})

    voteList = ayesList[0]

    ayes = []
    noes = []
    for voteList in ayesList:
        try:

            if "Ayes" in voteList.span.text:

                voteindex = ayesList.index(voteList)
                if "Ayes" not in ayesList[voteindex + 1].text:
                    ini_list = ayesList[voteindex + 1].text

                    res = ini_list.strip('][').split('; ')
                    if res != ["None"]:
                        ayes.extend(res)


        except:
            pass
    voteList = noesList[0]
    for voteList in noesList:
        try:

            if "Noes" in voteList.span.text:

                voteindex = noesList.index(voteList)
                if "Noes" not in noesList[voteindex + 1].text:
                    ini_list = noesList[voteindex + 1].text

                    res = ini_list.strip('][').split('; ')
                    if res != ["None"]:
                        noes.extend(res)


        except:
            pass
    votes = []
    for name in ayes:
        legislator = name
        vote = "aye"
        goverlytics_id = ""
        nothing = ""



        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', nothing,
                                                                 name_last=legislator)


        if gov_id is not None:
            goverlytics_id = gov_id

        v = {"legislator": legislator, "goverlytics_id": goverlytics_id, "vote": vote}
        votes.append(v)


    for name in noes:
        legislator = name
        vote = "noe"
        goverlytics_id = ""
        nothing = ""

        gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', nothing,
                                                             name_last=legislator)

        if gov_id is not None:
            goverlytics_id = gov_id
        v = {"legislator": legislator, "goverlytics_id": goverlytics_id, "vote": vote}
        votes.append(v)

    vote_data = {'date': date, 'description': description, 'yea': yea, 'nay': nay, 'nv': nv, 'absent': absent,
                 'total': total, 'passed': passed, 'chamber': chamber, 'votes': votes}
    print(vote_data)
    return vote_data


def collect_bill_details(bill_url):
    sponsors_id = []
    principal_sponsor_id = 0
    # print(bill_url)
    bill_title = ""
    sponsors = []
    principal_sponsor = ""
    actions = []
    site_topic = ""
    votes = []
    bill_type = ""
    psurl = ""
    bill_text = ""
    bill_description = ""
    bill_summary = ""

    # try:
    uClient = uReq(bill_url)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")

    # find bill title
    titlemain = page_soup.find("main", {"class": "col-12 body-content ncga-container-gutters"})
    titlediv = titlemain.findAll("div", {"class": "col-12"})
    titleloc = titlediv[1]
    bill_title = titleloc.a.text
    bill_link = "https://www.ncleg.gov" + titleloc.a["href"]

    # find type: resolution or bill

    typetext = page_soup.find("div", {"class": "col-12 col-sm-6 h2 text-center order-sm-2"})
    if "Bill" in typetext.text:
        bill_type = "Bill"
    elif "Resolution" in typetext.text:
        bill_type = "Resolution"
    else:
        bill_type = "Other"

    # find sponsors and principal sponsor
    sponsorsdiv = page_soup.findAll("div", {"class": "col-8 col-sm-9 col-xl-10 text-left pad-row"})
    # sponsordiv = sponsorsdiv[1]

    primsponList = []
    sponsor_urls = []
    for sponsordiv in sponsorsdiv:
        try:
            sponsoraList = sponsordiv.findAll("a")
            sponsora = sponsoraList[0]
            left_column_tags = sponsordiv.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if "(Primary)" in lefttag.text:
                    divTag = lefttag
                    aTags = lefttag.findAll("a")
                    ps = aTags[len(aTags) - 1]
                    principal_sponsor = ps.text
                    # get the url for the principal sponsor so we can merge on it to get their goverlytics id
                    psurl = "https://www.ncleg.gov" + ps["href"]
                    psid = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_last',
                                                                       principal_sponsor, source_url=psurl)
                    if psid is not None:
                        principal_sponsor_id = psid

            primsponList.append(sponsoraList)
            for sponsora in sponsoraList:
                sponsors.append(sponsora.text)
                psurl = "https://www.ncleg.gov" + sponsora["href"]
                sponsor_urls.append(psurl)







        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"

            message = template.format(type(ex).__name__, ex.args)

            # print(message)
    sponsors_id = []
    # for su in sponsor_urls:
    #
    #     if su in dbwork.psurls:
    #         suindex = dbwork.psurls.index(su)
    #         sponsors_id.append(dbwork.psids[suindex])
    sponsors_id = []
    for su in sponsor_urls:
        index = sponsor_urls.index(su)
        name_last = sponsors[index]
        hn = HumanName(name_last)
        if "." not in name_last:
            sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_last', name_last,
                                                                 source_url=su)
        else:
            first_initial = name_last.split(".")[0]
            last = name_last.split(".")[1].strip()
            sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', first_initial,
                                                                     name_last=last)





        # print(sponsor_id)
        # sponsor_id = scraper_utils.get_legislator_id(**search_for)

        # Some sponsor IDs weren't found, so we won't include these.
        # If you are unable to find legislators based on the provided search criteria, be
        # sure to investigate. Check the database and make sure things like names match
        # exactly, including case and diacritics.
        if sponsor_id is not None:
            sponsors_id.append(sponsor_id)

    # get actions
    actions = []
    cardbody = page_soup.findAll("div", {"class": "card-body"})
    cb = cardbody[0]

    for cb in cardbody:
        rows = (cb.findAll("div", {"class": "row"}))
        row = rows[0]

        for row in rows:

            try:
                date = row.find("div", {"class": "col-7 col-md-2 pr-0"}).text
                d = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")

                chamber = row.find("div", {"class": "col-7 col-md-1 col-lg-2 pr-0 text-nowrap"}).text
                description = row.find("div", {"class": "col-7 col-md-4 col-lg-3 pr-0"}).text

                action = {'date': d, 'action_by': chamber, 'description': description}
                actions.append(action)

                # get vote url, and vote data
                voteData = row.find("div", {"class": "col-7 col-md-2 order-2 order-md-0 pr-0"})
                voteLink = voteData.a["href"]
                voteLink = "https://www.ncleg.gov/" + voteLink

                vote_data = collect_vote_info(voteLink)

                votes.append(vote_data)

            except Exception as ex:

                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                # print(message)

    # get key words

    keywordsdiv = page_soup.findAll("div", {"class": "col-8 col-sm-9 col-xl-10 text-left scroll-column"})
    site_topic = keywordsdiv[1].text

    r = requests.get(bill_link)
    f = io.BytesIO(r.content)
    reader = PyPDF2.PdfFileReader(f)

    contents = reader.getPage(0).extractText()
    bill_text = contents

    # except Exception as ex:
    #
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     print(message)

    introducedIndex = len(actions) - 1
    try:
        date_introduced = actions[introducedIndex]['date']
        chamber_origin = actions[introducedIndex]['action_by']
    except:
        date_introduced = ""
        chamber_origin = ""

    bill_d = {'source_url': bill_url, 'bill_title': bill_title, 'bill_type': bill_type, 'sponsors': sponsors,
              'sponsors_id': sponsors_id, 'principal_sponsor': principal_sponsor,
              'principal_sponsor_id': principal_sponsor_id, 'current_status': "",
              'psurl': psurl, 'actions': actions, 'date_introduced': date_introduced,
              'chamber_origin': chamber_origin, 'session': '2020-2021', 'state': 'NC', 'state_id': '37',
              'site_topic': site_topic, 'votes': votes, 'committees': [], 'cosponsors': [], 'cosponsors_id': [],
              'topic': "", 'bill_text': bill_text, 'bill_description': bill_description, 'bill_summary': bill_summary,
              'country_id': scraper_utils.country_id, 'country': scraper_utils.country}

    return bill_d
#
# def add_topics(df):
#     # model_name = open(os.path.join(os.path.dirname(__file__), os.pardir,
#     #                                'C:\Users\anvo2\PycharmProjects\goverlytics-scrapers\topic_classifier.joblib'))
#     model_name = 'topic_classifier.joblib'
#     print('Loading model...')
#
#     clf = load(model_name)
#
#     print('Model loaded.')
#
#     df.loc[df.bill_text == "", 'bill_text'] = df.loc[df.bill_text == "", 'site_topic']
#     # cast principal sponsor id to int
#     # df['bill_text'] = df['bill_text'].replace({np.nan: None})
#     # df['bill_summary'] = [str(d) if d else d for d in df['bill_summary']]
#
#     words = stopwords.words('english')
#     stemmer = WordNetLemmatizer()
#     # for bt in df['bill_text']:
#     #     print(type(bt))
#     df['processedtext'] = df['bill_text'].apply(
#         lambda x: ' '.join(
#             [stemmer.lemmatize(i) for i in re.sub('[^a-zA-Z]', ' ', x).split() if i not in words]).lower())
#
#     df['topic'] = clf.predict(df['processedtext'])
#     df['bill_text'] = df['processedtext']
#     print(df)
#
#     return df



if __name__ == '__main__':
    # this is only 50 urls right now?
    billinfos = collect_bill_urls('https://www.ncleg.gov/Legislation/Bills/ByKeyword/2021/All')
    billinfos = billinfos[:100]
    smalldf = pd.DataFrame(billinfos[:100])

    # print(billinfos)
    links = [d['source_url'] for d in billinfos]
    lessLinks = links[:100]
    link = links[0]

    #
    # billLink = billinfos[1]["url"]
    # print(billLink)

    # bill_data = []
    # for billLink in lessLinks:
    #     try:
    #         (app.collect_bill_details(billLink))
    #     except:
    #         print("Exception")
    #
    # (app.collect_bill_details(link))

    with Pool() as pool:
        # #
        bill_data = pool.map(func=collect_bill_details, iterable=links)
    # #
    maindf = pd.DataFrame(bill_data)

    # print(maindf)
    # print("Process complete")

    # mainwithid = maindf.merge(dbwork.id_df, on='psurl', how='inner')

    big_df = pd.merge(maindf, smalldf, how='left', on="source_url")

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    big_df = big_df.drop(['psurl'], axis=1)
    big_df['source_id'] = ""
    # big_df = add_topics(big_df)
    # big_df = topics.add_topics(big_df)
    print(big_df)
    big_list_of_dicts = big_df.to_dict('records')
    # print(*big_list_of_dicts, sep="\n")

    print('Writing data to database...')
    scraper_utils.insert_legislation_data_into_db(big_list_of_dicts)

    print('Complete!')

