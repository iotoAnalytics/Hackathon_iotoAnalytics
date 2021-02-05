"""
Author: Avery Quan
Date Created: Jan 28, 2021
Function: Scrape legislation data for the state of Alabama
Notes:
    - Url field isn't very useful as the Alabama website requires a session and will redirect you if you don't have one, it is also NOT UNIQUE, 
        the page depends on the session year you have chosen.
    - Currently, session year is hardcoded. 
            -Note that prefilled_bills_url is also hardcoded, the date syntax can be found on the Alabama session info page
    - Could not figure out how to access Resolutions page with bs4. Can be done through Selenium, but will be very slow. i've left it un
        scraped, as I believe it is a simple problem to fix for someone with more experience.



Notes on how the website works: (ASP.NET)

1. This site requires a session in order to access certain pages, if you do not have one, you will be redirected to a default page where you can choose your session.
	This applies to bs4 calls as well. You can maintain a session by creating a requests.Session() object, and getting the html by using session.get(url)

2. Page Navigation: ASP.NET websites have some javascript called postback functions. This means, you have to send a POST request with certain arguments to a certain url 
	before you can access certain types of pages. The arguments you will need are __EVENTTARGET and __EVENTARGUMENT, as well as the url to send the POST to. 
    On this particular site, __EVENTTARGET and __EVENTARGUMENT can be found by inspecting the input/link and using the tag name and id respectively. In some cases,
    the function will be called in the tag:
    
    javascript:__doPostBack('ctl00$ContentPlaceHolder1$gvBills','Bill$0')

    The first argument is __EVENTTARGET and the second is __EVENTARGUMENT. The url is the page that holds the links. Once you have these,
    send the POST request and you should be able to access that page. POST requests can be found in functions starting with set_

"""

import sys, os
from pathlib import Path

from requests.models import parse_header_links

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
import configparser
import requests
from bs4 import BeautifulSoup
import pandas as pd
from database import Database
from legislation_scraper_utils import LegislationScraperUtils, LegislationRow
from multiprocessing import Pool
from itertools import product
import datetime
import numpy

# Testing imports
from pprint import pprint
import pickle

# Hardcoded for now
current_session = 'Regular Session 2019'
session_year = '2019'
chambers = {'S': 'Senate', 'H' : 'House'}


headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
# URLs to POST to
select_session_url = 'http://alisondb.legislature.state.al.us/Alison/SelectSession.aspx'
prefiled_bills_url = 'http://alisondb.legislature.state.al.us/Alison/SESSBillsList.aspx?SELECTEDDAY=1:2019-03-05&BODY=1753&READINGTYPE=R1&READINGCODE=B&PREFILED=Y'
bills_sponsor_senate_url = 'http://alisondb.legislature.state.al.us/alison/SESSBillsBySenateSponsorSelect.aspx'
bills_sponsor_house_url = 'http://alisondb.legislature.state.al.us/alison/SESSBillsByHouseSponsorSelect.aspx'
reso_sponsor_senate_url = 'http://alisondb.legislature.state.al.us/Alison/SESSResosBySenateSponsorSelect.aspx'

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')
state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))


scraper_utils = LegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

def set_session():
    # change event argument to change session year, currently set to 2019
    session_payload = {"__EVENTTARGET":"ctl00$ContentPlaceHolder1$gvSessions", "__EVENTARGUMENT": "$3"}
    
    session.post(select_session_url, session_payload, headers)

def set_chamber(chamber):
    if chamber == 'Senate':
        # switch to senate session
        senate_payload = {"__EVENTTARGET":"ctl00$ContentPlaceHolder1$btnSenate", "__EVENTARGUMENT": "Senate"}
        session.post('http://alisondb.legislature.state.al.us/Alison/SessPrefiledBills.aspx', senate_payload, headers)

    elif chamber == 'House':
        # switch to house session
        house_payload = {"__EVENTTARGET":"ctl00$ContentPlaceHolder1$btnHouse", "__EVENTARGUMENT": "House"}
        session.post('http://alisondb.legislature.state.al.us/Alison/SessPrefiledBills.aspx', house_payload, headers)

    else:
        raise Exception("Incorrect chamber")


def set_vote_session(table_row, bill_name):
    # Look at votes in table row 
    # ctl00$ContentPlaceHolder1$gvHistory$ctl0 [insert table row + 2 here] $ctl00              
    target = 'ctl00$ContentPlaceHolder1$gvHistory$ctl' + str(table_row + 2).zfill(2) + '$ctl00'
    # Select$ [insert table row here]  
    argument = 'Select$' + str(table_row)
    url = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=' + bill_name + '&WIN_TYPE=BillResult'

  
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


        table = table.replace(numpy.nan, '', regex=True )
        for index, row in table.iterrows():
            bill_name = row['Bill']
            fields = scraper_utils.initialize_row()
            fields.session = current_session
            
            bill_state_id = (current_session.replace(' Session ', '') + '_' + row['Bill']).replace(' ', '')
            fields.bill_state_id = bill_state_id
            fields.goverlytics_id = f'AL_{bill_state_id}'
            fields.bill_name = row['Bill']
            fields.bill_summary = row['Unnamed: 7']
            fields.date_collected = datetime.datetime.now()
            fields.site_topic = row['Subject']
            fields.current_status = row['Status']
            fields.chamber_origin = chamber
            fields.principal_sponsor = row['Sponsor']
            fields.sponsors.append(row['Sponsor'])
            fields.bill_type = bill_type
            fields.url = f'/us/AL/legislation/{bill_state_id}'


            bills[row['Bill']] =  fields

        return bills
    except ImportError:
        print(f'The page at this url probably has no table of bills to scrape: ' + bills_url)
        

def scrape_bills_detailed(fields):
    url_first = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL='
    url_last = '&WIN_TYPE=BillResult'
    keys = fields.keys()
    for key in keys:
        url_base = url_first + key + url_last
        page = session.get(url_base)
        member_soup = BeautifulSoup(page.text, 'lxml')
        members = member_soup.find('table', id='ContentPlaceHolder1_gvHistory')
        try:
            table = pd.read_html(str(members))[0]
            fields[key].state_url = url_base
            
            vote_rows = table.index[table['Vote'] == table['Vote']].tolist()
            if ~ bool(vote_rows):
                vote_num = [x.replace('Roll ', '') for x in table['Vote'][vote_rows].values.tolist()]

                fields[key].votes = scrape_votes(vote_rows, vote_num, key, table)

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
                        fields[key].actions.append({'date': nan(table['Calendar Date'][index]), 'action_by': chambers[table['Body'][index]], 'description': table['Matter'][index]})
                    if   table['Committee'][index] == table['Committee'][index]:
                        fields[key].committees = {'committee': members.find('td',text=table['Committee'][index])['title'], 'chamber': chambers[table['Body'][index]]}
            except KeyError:
                # probably has a blank row
                pass
        except ImportError:
            print('Likely a table not found at url: ' + url_base)

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
    for index in range (0, length):
        set_vote_session(columns[index], bill_name)

        chamber = table['Body'][columns[index]]
        base_url = f'http://alisondb.legislature.state.al.us/Alison/GetRollCallVoteResults.aspx?VOTE={nums[index]}&BODY={chamber}&INST={bill_name}&SESS=1074&AMDSUB=&nbsp;'

        page = session.get(base_url)
        member_soup = BeautifulSoup(page.text, 'lxml')
        members = member_soup.find('table', {'id':'ContentPlaceHolder1_gv1'})
        vote_table = pd.read_html(str(members))[0]
        
        vote = {}
        vote['date'] = table['Calendar Date'][columns[index]]
        vote['description'] = table['Matter'][columns[index]]
        vote['chamber'] = chambers[chamber]
        vote['votes'] = count_votes(vote_table)

        votes.append(vote)

    return votes

def count_votes(table):

    total_votes = {'yea' : 0, 'nay' : 0, 'no vote' : 0, 'absent' : 0, 'total' : 0}
    abbreviation = {'Y' : 'yea', 'N' : 'nay', 'A' : 'no vote', 'P' : 'absent'}
    votes = []

    for index, value in table['Vote'].iteritems():
        try:
            vote = {}
            vote['voted'] = abbreviation[value]
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
    session_payload = {"__EVENTTARGET":'ctl00$ContentPlaceHolder1$btnSponsor', "__EVENTARGUMENT": "ContentPlaceHolder1_btnSponsor"}
    session.post(target_url, session_payload, headers)

    people = session.get(target_url)
    member_soup = BeautifulSoup(people.text, 'lxml')
    sponsors = member_soup.find_all('input', type='image')

    for index, sponsor in enumerate(sponsors):
        try:
            # Set session for bill by Sponsor
            session_payload = {"__EVENTTARGET":f'ctl00$ContentPlaceHolder1$ImageButton{index + 1}', "__EVENTARGUMENT": f"ContentPlaceHolder1_ImageButton{index + 1}"}
            session.post(target_url, session_payload, headers)
            name = sponsor['src'].split('/')[-1].split('_')[0]
            oid = sponsor['alt']
            
            sponsor_url = f'http://alisondb.legislature.state.al.us/alison/SESSBillsList.aspx?NAME={name}&SPONSOROID={oid}&BODY=1755&SESSNAME=Regular%20Session%20{session_year}'
            # if bill_type == 'Resolution':
            #     sponsor_url = f'http://alisondb.legislature.state.al.us/Alison/SESSResosList.aspx?NAME={name}&SPONSOROID={oid}&BODY=1753&SESSNAME=Regular%20Session%20{session_year}'
            #     # Set session By Sponsor
            #     session_payload = {"__EVENTTARGET":f'ctl00$ContentPlaceHolder1$Img{index + 1}', "__EVENTARGUMENT": f"ContentPlaceHolder1_Img{index + 1}"}
            #     session.post(target_url, session_payload, headers)

            bills = scrape_bills(chamber, sponsor_url, bill_type)
            if bills is not None:
                for key, bill in bills.items():
                    bill.principal_sponsor_id = int(oid)
                    bill.sponsors_id.append(int(oid))

                list_bills = {**list_bills, **bills}
        except KeyError:
            # likely a unfinished legislator page
            pass

    return list_bills


def scrape_co_sponsors(fields, target_url):
    # Set session By co sponsor
    session_payload = {"__EVENTTARGET":'ctl00$ContentPlaceHolder1$btnCoSponsor', "__EVENTARGUMENT": "ContentPlaceHolder1_btnCoSponsor"}
    session.post(target_url, session_payload, headers)

    people = session.get(target_url)
    member_soup = BeautifulSoup(people.text, 'lxml')
    sponsors = member_soup.find_all('input', type='image')
    return get_cosponsor_url(fields, sponsors)
    

def get_cosponsor_url(fields, sponsors):

    for sponsor in sponsors: 
        try:
            name = sponsor['src'].split('/')[-1].split('_')[0]
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
                fields[bill].cosponsors.append(name)
                fields[bill].cosponsors_id.append(int(oid))
                
                    
        except Exception as e:
            print(e)
            print('Blank legislator at ' + sponsor['src'])
            
    return fields

# used for debugging functions that rely on data to already exist
def save_file(my_list, name):
    with open(name, 'wb') as f:
        pickle.dump(my_list, f)

def open_file(name):
    with open(name, 'rb') as f:
        my_list = pickle.load( f)
        return my_list

session = requests.Session()
set_session()
set_chamber('Senate')

fields= {}

if __name__ == '__main__':  
    with Pool() as pool:

        fields = pool.starmap(scrape_bills, [('Senate', prefiled_bills_url, 'Prefiled Bill')])[0]
        fields = pool.starmap(scrape_sponsors, [('Senate', bills_sponsor_senate_url, 'Bill')])[0]
        fields = pool.starmap(scrape_co_sponsors, [(fields, bills_sponsor_senate_url)])
        fields = pool.map(scrape_bills_detailed, fields)[0]


        # save_file(fields, 'data.txt')
        # fields = open_file()[0]
        set_chamber('House')
        fields_house = {}

        fields_house = pool.starmap(scrape_bills, [('House', prefiled_bills_url, 'Prefiled Bill')])[0]
        fields_house = pool.starmap(scrape_sponsors, [('House', bills_sponsor_house_url, 'Bill')])[0]
        fields_house = pool.starmap(scrape_co_sponsors, [(fields_house, bills_sponsor_house_url)])
        fields_house = pool.map(scrape_bills_detailed, fields_house)[0]

        pool.close()
        pool.join()

    # save_file(fields_house, 'data2.txt')
    # fields_house = open_file('data2.txt')[0]


    scraper_utils.insert_legislation_data_into_db(list(fields_house.values()))
    scraper_utils.insert_legislation_data_into_db(list(fields.values()))
