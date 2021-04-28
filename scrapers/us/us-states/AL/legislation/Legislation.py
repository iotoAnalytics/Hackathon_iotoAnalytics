"""
Author: Avery Quan
Date Created: Feb 4, 2021
Function: Scrape historical legislation data for the state of Alabama for ALL years
Issues:

    - Could not figure out how to access Resolutions page with bs4. Usual post arguments do not work. Can be done through Selenium, 
        but will be very slow. i've left it unscraped, as I believe it is a simple problem to fix for someone with more experience.
        

Notes:
    - Url field isn't very useful as the Alabama website requires a session and will redirect you if you don't have one, it is also NOT UNIQUE, 
        the page depends on the session year you have chosen.

    - Website server is not always reliable and you will occasionally get a missing page error, I've put solutions in place for bills but this could 
        be the issue for future errors.

    - Runtime: Approx 4 hours



Notes on how the website works: (ASP.NET)

1. This site requires a session in order to access certain pages, if you do not have one, you will be redirected to a default page where you can choose your session.
	This applies to bs4 calls as well. You can maintain a session by creating a requests.Session() object, and getting the html by using session.get(url)

2. Page Navigation: ASP.NET websites have some javascript called postback functions. This means, you have to send a POST request with certain arguments to a certain url 
	before you can access certain types of pages. The arguments you will need are __EVENTTARGET and __EVENTARGUMENT, as well as the url to send the POST to. 
    On this particular site, __EVENTTARGET and __EVENTARGUMENT can be found by inspecting the input/link and using the tag name and id respectively. In some cases,
    the function will be called in the tag:
    
    javascript:__doPostBack('ctl00$ContentPlaceHolder1$gvBills','Bill$0')

    The first argument is __EVENTTARGET and the second is __EVENTARGUMENT. The url is the page that holds the links. Once you have these,
    send the POST request and you should be able to access that page. Example of a POST request can be found in set_session()   

"""
import sys
import os
from pathlib import Path

from requests.models import parse_header_links

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import configparser
import requests
from bs4 import BeautifulSoup
import pandas as pd
from database import Database
from scraper_utils import USStateLegislationScraperUtils
from multiprocessing import Pool
from itertools import product
import datetime
import numpy


# Testing imports
# from pprint import pprint
# import pickle

# Hardcoded for now
current_session = 'Regular Session 2019'
session_year = '2019'
chambers = {'S': 'Senate', 'H': 'House'}


headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
# URLs to POST to
select_session_url = 'http://alisondb.legislature.state.al.us/Alison/SelectSession.aspx'
bills_sponsor_senate_url = 'http://alisondb.legislature.state.al.us/alison/SESSBillsBySenateSponsorSelect.aspx'
bills_sponsor_house_url = 'http://alisondb.legislature.state.al.us/alison/SESSBillsByHouseSponsorSelect.aspx'
reso_sponsor_senate_url = 'http://alisondb.legislature.state.al.us/Alison/SESSResosBySenateSponsorSelect.aspx'

# Initialize config parser and get variables from config file
# configParser = configparser.RawConfigParser()
# configParser.read('config.cfg')
state_abbreviation = 'AL'
database_table_name = 'us_al_legislation_test'
legislator_table_name = 'us_al_legislators'

# #Initialize database and scraper utils
# db_user = str(configParser.get('databaseConfig', 'db_user'))
# db_pass = str(configParser.get('databaseConfig', 'db_pass'))
# db_host = str(configParser.get('databaseConfig', 'db_host'))
# db_name = str(configParser.get('databaseConfig', 'db_name'))

# Database.initialise(database=db_name, host=db_host, user=db_user, password=db_pass)


scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay(select_session_url)
# default sess_column scrapes current year only


def set_session(sess_column):
    session_payload = {"__EVENTTARGET": "ctl00$ContentPlaceHolder1$gvSessions",
                       "__EVENTARGUMENT": f"${sess_column}"}

    session.post(select_session_url, session_payload, headers)


def set_chamber(chamber):
    if chamber == 'Senate':
        # switch to senate session
        senate_payload = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnSenate", "__EVENTARGUMENT": "Senate"}
        session.post(
            'http://alisondb.legislature.state.al.us/Alison/SessPrefiledBills.aspx', senate_payload, headers)

    elif chamber == 'House':
        # switch to house session
        house_payload = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnHouse", "__EVENTARGUMENT": "House"}
        session.post(
            'http://alisondb.legislature.state.al.us/Alison/SessPrefiledBills.aspx', house_payload, headers)

    else:
        raise Exception("Incorrect chamber")


def set_vote_session(table_row, bill_name):
    # Look at votes in table row
    # ctl00$ContentPlaceHolder1$gvHistory$ctl0 [insert table row + 2 here] $ctl00
    target = 'ctl00$ContentPlaceHolder1$gvHistory$ctl' + \
        str(table_row + 2).zfill(2) + '$ctl00'
    # Select$ [insert table row here]
    argument = 'Select$' + str(table_row)
    url = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=' + \
        bill_name + '&WIN_TYPE=BillResult'

    votes_payload = {"__EVENTTARGET": target, "__EVENTARGUMENT": argument}
    session.post(url, votes_payload, headers)


def scrape_bills(chamber, bills_url, bill_type):
    bills = {}

    page = session.get(bills_url)
    member_soup = BeautifulSoup(page.text, 'lxml')
    member = member_soup.find('table')
    try:
        table = pd.read_html(str(member))[1]

        bill_names = member.find_all('input')

        sponsor_names = [x for x in bill_names]
        # delete even values, which are bill names
        del sponsor_names[0::2]
        sponsor_names = [x['value'] for x in sponsor_names]
        table['Sponsor'] = sponsor_names

        # delete even values, which are the sponsor names
        del bill_names[1::2]
        bill_names = [x['value'] for x in bill_names]
        table['Bill'] = bill_names

        table = table.replace(numpy.nan, '', regex=True)
        for index, row in table.iterrows():
            fields = scraper_utils.initialize_row()
            fields.session = current_session

            source_id = (current_session.replace(
                ' Session ', '') + '_' + row['Bill']).replace(' ', '')
            fields.source_id = source_id
            fields.goverlytics_id = f'AL_{source_id}'
            fields.bill_name = row['Bill']
            fields.bill_summary = row['Unnamed: 7']
            fields.date_collected = datetime.datetime.now()
            fields.source_topic = row['Subject']
            fields.current_status = row['Status']
            fields.chamber_origin = chamber
            fields.principal_sponsor = row['Sponsor'].split(
                ' ')[0].split('(')[0]
            fields.sponsors.append(row['Sponsor'].split(' ')[0].split('(')[0])
            fields.bill_type = bill_type
            fields.committees = []
            # fields.url = f'/us/AL/legislation/{source_id}'

            bills[row['Bill']] = fields

        return bills
    except ImportError:
        print(
            f'The page at this url probably has no table of bills to scrape: ' + bills_url)


def scrape_bills_detailed(fields):
    url_first = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL='
    url_last = '&WIN_TYPE=BillResult'
    keys = list(fields.keys())
    keys_len = len(keys)
    count = 0
    unsuccess_attempts = 0
    while count < keys_len:

        key = keys[count]
        count += 1

        url_base = url_first + key + url_last
        page = session.get(url_base)
        member_soup = BeautifulSoup(page.text, 'lxml')
        members = member_soup.find('table', id='ContentPlaceHolder1_gvHistory')
        try:
            table = pd.read_html(str(members))[0]
            fields[key].source_url = url_base

            vote_rows = table.index[table['Vote'] == table['Vote']].tolist()
            if ~ bool(vote_rows):
                vote_num = [x.replace('Roll ', '')
                            for x in table['Vote'][vote_rows].values.tolist()]

                fields[key].votes = scrape_votes(
                    vote_rows, vote_num, key, table)

            len_table = len(table)
            try:
                date_index = 0
                for index in range(0, len_table):
                    if index == date_index:
                        if pd.isna(table['Calendar Date'][index]):
                            date_index += 1
                        else:
                            fields[key].date_introduced = table['Calendar Date'][index]

                    if not pd.isna(table['Matter'][index]):
                        fields[key].actions.append({'date': nan(
                            table['Calendar Date'][index]), 'action_by': chambers[table['Body'][index]], 'description': table['Matter'][index]})
                    if table['Committee'][index] == table['Committee'][index]:
                        fields[key].committees.append({'committee': members.find('td', text=table['Committee'][index])[
                                                      'title'], 'chamber': chambers[table['Body'][index]]})
                unsuccess_attempts = 0
            except KeyError:
                # probably has a blank row
                pass
        except ImportError as e:
            # If this doesnt print out twice for a bill, the error was fixed
            print('Likely a table not found at url: ' + url_base)
            unsuccess_attempts += 1
            # Either the table actually doesn't exist, or there is just a server error on their side. We can try again to see if we can get the page.
            # Chance of a server error seems relatively low ~ 2% im guessing but you can raise the cap for unsuccess_attempts for guranteed bill scraping
            # at the cost of higher runtime
            if unsuccess_attempts > 1:
                unsuccess_attempts = 0
            else:
                count -= 1
            continue

    return fields


def nan(object):
    if object == object:
        return object

    return ''


def scrape_votes(columns, nums, bill_name, table):
    # Session does not seem to matter, without MOID you will have to count the votes yourself, I don't know how to calculate the MOID
    # but a pattern I noticed is that consecutive vote number's MOID increases by 4. This may not hold for big increases in vote number
    # example url  'http://alisondb.legislature.state.al.us/Alison/GetRollCallVoteResults.aspx?MOID=606437&VOTE=46&BODY=S&INST=SB5&SESS=1074&AMDSUB=&nbsp;'

    length = len(columns)

    votes = []
    for index in range(0, length):
        set_vote_session(columns[index], bill_name)

        chamber = table['Body'][columns[index]]
        base_url = f'http://alisondb.legislature.state.al.us/Alison/GetRollCallVoteResults.aspx?VOTE={nums[index]}&BODY={chamber}&INST={bill_name}&SESS=1074&AMDSUB=&nbsp;'

        page = session.get(base_url)
        member_soup = BeautifulSoup(page.text, 'lxml')
        members = member_soup.find('table', {'id': 'ContentPlaceHolder1_gv1'})
        vote_table = pd.read_html(str(members))[0]

        vote = {}
        vote['date'] = table['Calendar Date'][columns[index]]
        vote['description'] = table['Matter'][columns[index]]
        vote['chamber'] = chambers[chamber]
        vote['votes'] = count_votes(vote_table)

        votes.append(vote)

    return votes


def count_votes(table):

    total_votes = {'yea': 0, 'nay': 0, 'no vote': 0, 'absent': 0, 'total': 0}
    abbreviation = {'Y': 'yea', 'N': 'nay', 'A': 'no vote', 'P': 'absent'}
    votes = []

    for index, value in table['Vote'].iteritems():
        try:
            vote = {}
            vote['voted'] = abbreviation[value]
            if pd.isna(table['Member'][index]):
                vote['legislator'] = 'Unnamed Member'
            else:
                vote['legislator'] = table['Member'][index]
            votes.append(vote)

            total_votes[abbreviation[value]] += 1
            total_votes['total'] += 1
        except:
            pass

    for index, value in table['Vote.1'].iteritems():
        try:
            vote = {}
            vote['voted'] = abbreviation[value]
            if pd.isna(table['Member.1'][index]):
                vote['legislator'] = 'Unnamed Member'
            else:
                vote['legislator'] = table['Member.1'][index]
            votes.append(vote)

            total_votes[abbreviation[value]] += 1
            total_votes['total'] += 1
        except:
            pass

    return (votes, total_votes)


def scrape_sponsors(chamber, target_url, bill_type):
    list_bills = {}
    # Set session By Sponsor
    session_payload = {"__EVENTTARGET": 'ctl00$ContentPlaceHolder1$btnSponsor',
                       "__EVENTARGUMENT": "ContentPlaceHolder1_btnSponsor"}
    session.post(target_url, session_payload, headers)

    people = session.get(target_url)
    member_soup = BeautifulSoup(people.text, 'lxml')
    sponsors = member_soup.find_all('input', type='image')
    sponsor_names = member_soup.find_all(
        'span', attrs={'class': 'label label-default', 'style': 'display:inline-block;'})
    for index, sponsor in enumerate(sponsors):
        try:
            # Set session for bill by Sponsor
            session_payload = {"__EVENTTARGET": f'ctl00$ContentPlaceHolder1$ImageButton{index + 1}',
                               "__EVENTARGUMENT": f"ContentPlaceHolder1_ImageButton{index + 1}"}
            session.post(target_url, session_payload, headers)
            name = (sponsor_names[index].string).replace(' ', '%20')
            oid = sponsor['alt']
            sponsor_url = f'http://alisondb.legislature.state.al.us/alison/SESSBillsList.aspx?NAME={name}&SPONSOROID={oid}&BODY=1755&SESSNAME=Regular%20Session%20{session_year}'

            bills = scrape_bills(chamber, sponsor_url, bill_type)
            if bills is not None:
                for key, bill in bills.items():

                    kwargs = {"source_id": oid}
                    gover_id = scraper_utils.get_legislator_id(**kwargs)

                    bill.principal_sponsor_id = gover_id
                    bill.sponsors_id.append(gover_id)

                list_bills = {**list_bills, **bills}
        except Exception as e:
            # likely a unfinished legislator page at bottom -> leads to index out of range
            print(f'Exception: {e.__class__.__name__}')
            print(f'With exception values: {e}')

    return list_bills


def scrape_co_sponsors(fields, target_url):
    # Set session By co sponsor
    session_payload = {"__EVENTTARGET": 'ctl00$ContentPlaceHolder1$btnCoSponsor',
                       "__EVENTARGUMENT": "ContentPlaceHolder1_btnCoSponsor"}
    session.post(target_url, session_payload, headers)

    people = session.get(target_url)
    member_soup = BeautifulSoup(people.text, 'lxml')
    sponsors = member_soup.find_all('input', type='image')
    sponsor_names = member_soup.find_all(
        'span', attrs={'class': 'label label-default', 'style': 'display:inline-block;'})
    return get_cosponsor_url(fields, sponsors, sponsor_names)


def get_cosponsor_url(fields, sponsors, sponsor_names):

    for index, sponsor in enumerate(sponsors):
        try:
            name = (sponsor_names[index].string).replace(' ', '%20')
            # name = sponsor['src'].split('/')[-1].split('_')[0]
            oid = sponsor['alt']
            sponsor_url = f'http://alisondb.legislature.state.al.us/alison/SESSBillsList.aspx?NAME={name}&SPONSOROID={oid}&BODY=1755&SESSNAME=Regular%20Session%20{session_year}'
            page = session.get(sponsor_url)
            member_soup = BeautifulSoup(page.text, 'lxml')
            member = member_soup.find('table')
            bill_names = member.find_all('input')
            # delete even values, which are the sponsor names
            del bill_names[1::2]
            bill_names = [x['value'] for x in bill_names]

            for bill in bill_names:
                fields[bill].cosponsors.append(
                    name.split('%20')[0].split('(')[0])

                kwargs = {"source_id": oid}
                gover_id = scraper_utils.get_legislator_id(**kwargs)

                fields[bill].cosponsors_id.append(gover_id)

        except Exception as e:
            print(f'Exception {e.__class__.__name__}')
            print(f'With exception values {e}')
            print('Blank legislator at ' + sponsor['src'])
            continue

    return fields

# used for debugging functions that rely on data to already exist
# def save_file(my_list, name):
#     with open(name, 'wb') as f:
#         pickle.dump(my_list, f)

# def open_file(name):
#     with open(name, 'rb') as f:
#         my_list = pickle.load( f)
#         return my_list


def scrape():

    print('step sponsor: ' + current_session)
    fields = scrape_sponsors('Senate', bills_sponsor_senate_url, 'Bill')
    print('step cosponsor: ' + current_session)
    fields = scrape_co_sponsors(fields, bills_sponsor_senate_url)

    print('step bills: ' + current_session)
    fields = scrape_bills_detailed(fields)

    # for debugging to save time
    # save_file(fields, 'data.txt')
    # fields = open_file()[0]
    fields_house = {}
    print('step sponsor: ' + current_session)
    fields_house = scrape_sponsors('House', bills_sponsor_house_url, 'Bill')

    print('step cosponsor: ' + current_session)
    fields_house = scrape_co_sponsors(fields_house, bills_sponsor_house_url)

    print('step bills: ' + current_session)
    fields_house = scrape_bills_detailed(fields_house)

    # for debugging to save time
    # save_file(fields_house, 'data2.txt')
    # fields_house = open_file('data2.txt')[0]

    print('step Insert: ' + current_session)
    scraper_utils.write_data(list(fields_house.values()))
    scraper_utils.write_data(list(fields.values()))

    # for d in list(fields_house.values())[:10]:
    #     print(d)

    # for d in list(fields.values())[:10]:
    #     print(d)


session = requests.Session()

page = session.get(select_session_url)
member_soup = BeautifulSoup(page.text, 'lxml')
member = member_soup.find('table', id='ContentPlaceHolder1_gvSessions')
members = member.find_all('td')
num_sessions = len(members)
# num_sessions corresponds to the session years, to exclude scraping current year, change 0 in for loop to 1.
# session numbers can be found at http://alisondb.legislature.state.al.us/Alison/SelectSession.aspx
# and by inspecting the session link, it will be the second argument in the postback function
for session_no in range(1, 2):
    current_session = members[session_no].text
    session_year = members[session_no].text.split(' ')[-1]
    # print(current_session)
    set_session(session_no)
    scrape()
