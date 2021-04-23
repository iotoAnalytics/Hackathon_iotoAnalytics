'''
Author: Avery Quan
Written: April 5, 2021
Description:
 Scrapes historical and current legislation data for the state of Arkansas

 Notes: 

 - Introduction Date in earlier years does not seem to be accurate, all of them cite 1900 as the introduction date.

'''
# from _typeshed import NoneType
import sys, os
from pathlib import Path
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import traceback
import tqdm

# set path to current file directory
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

from legislation_scraper_utils import LegislationScraperUtils, LegislationRow, USFedLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait   
from selenium.webdriver.support import expected_conditions as EC
import pickle
from datetime import datetime

import pdfplumber
import requests 
import io


# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get(
    'scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get(
    'scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get(
    'scraperConfig', 'legislator_table_name'))

scraper_utils = LegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

scraper_utils = USFedLegislationScraperUtils( database_table_name, legislator_table_name)
site_url = 'https://www.arkleg.state.ar.us'

#used for memoization when getting goverlytics id
gov_ids = {}

#used for debugging functions that rely on data to already exist
def save_file(my_list, name):
    with open(name, 'wb') as f:
        pickle.dump(my_list, f)

def open_file(name):
    with open(name, 'rb') as f:
        my_list = pickle.load( f)
        return my_list


def get_urls_session(historical = False):
    current_year = datetime.year
    session_url = f'https://www.arkleg.state.ar.us/Acts/SearchByRange?startAct=1&endAct=1000&keywords=&ddBienniumSession={current_year}%2F{current_year}R#SearchResults'
    
    page = requests.get(session_url)    
    soup = BeautifulSoup(page.content, 'html.parser')
    sessions = soup.find('select', id='ddBienniumSession').find_all('option')[1:]

    if not historical:
        sessions = [sessions[0]]

    session_urls = []
    bill_types = ['HB', 'HJR', 'HR', 'HCR', 'HMR', 'SB', 'SJR', 'SR', 'SMR', 'SCR', 'SCMR']
    for session in sessions:
        for bill_type in bill_types:
            year, code = session['value'].split('/')
            
            url = f'type={bill_type}&ddBienniumSession={year}%2F{code}'
            if len(code) < 4:
                code = year + code
            session_urls.append((code , url))

    return session_urls

def get_urls_bills(session_urls):
    url_base = 'https://www.arkleg.state.ar.us/Bills/ViewBills?start='
    urls = []
    for url in session_urls:
        web_page = requests.get(url_base + '&' + url[1])    
        soup = BeautifulSoup(web_page.content, 'html.parser')
        div = soup.find('div', class_='col-sm-12 col-md-12 col-lg-12')
        try:
            last_page = div.find_all('a')[-1]['href'].split('&')[0].replace('?start=', '')
        except IndexError:
            last_page = 0

        page_start = 0
        while page_start <= int(last_page):
            page = requests.get(url_base + str(page_start) + '&' + url[1])    
            soup = BeautifulSoup(page.content, 'html.parser')
            divs = soup.find_all('div', class_='measureTitle')
            a = [(url[0], div.find('a')['href']) for div in divs]
            urls += a
            page_start += 20

        
    return urls

def get_gov_id(url):
    global gov_ids
    if url in gov_ids:
        return gov_ids[url]

    page = requests.get(site_url + url)
    soup = BeautifulSoup(page.content, 'html.parser')
    h1 = soup.find('h1')
    
    name = HumanName(' '.join(h1.text.split(' ')[1:-1]).split(',')[0])

    gov_id = scraper_utils.get_legislator_id(name_first=name.first, name_last= name.last)

    gov_ids[url] = gov_id
    return gov_id

def get_votes(url, timestamp, desc, chamber, ):
    vote_dict = {'Yeas':'yea', 'Nays':'nay', 'Non Voting':'nv', 'Present':'absent', 'Excused':'absent'}
    date = datetime.strptime(timestamp, '%m/%d/%Y')
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    votes_html = soup.find_all('h3')[1:]
    votes_total = []
    for vote in votes_html:
        votes_total.append(int(vote.text.split(':')[1]))

    grid = soup.find_all('div', role='grid')
    votes = []
    for vote in grid:
        voted = vote.find_previous('div').text.split(':')[0].strip()
        legislators = vote.find_all('a')
        for legislator in legislators:
            name = legislator.text.split('.')
            if  len(name) == 2:
                name = name[1].strip()
            else:
                name = name[0].strip()
            try :
                votes.append({'goverlytics_id': get_gov_id(legislator['href']), 'legislator': name, 'voted': vote_dict[voted]})
            except AttributeError:
                if  legislator['href'] == '/Legislators/Detail?member=Beatty+Jr.&ddBienniumSession=2021%2F2021R':
                    votes.append({'goverlytics_id': get_gov_id('/Legislators/Detail?member=Beaty+Jr.&ddBienniumSession=2021%2F2021R'), 'legislator': name, 'voted': vote_dict[voted]})
                else:
                    print(f'{legislator.text.strip()} at {url} will have its gov_id left blank as the url is mispelt')
                    votes.append({'goverlytics_id': '', 'legislator': legislator.text, 'voted': voted})

    try:
        return {'date':date.date(), 'description':desc, 'chamber': chamber, 'yea':votes_total[0], 'nay':votes_total[1], 
            'nv':votes_total[2] + votes_total[3], 'absent': votes_total[4], 'total': sum(votes_total),
            'passed': int(votes_total[0] > votes_total[1] + votes_total[2] + votes_total[3]), 'votes':votes }
    except IndexError:
        # No voting data
        pass

def scrape(info):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    
    url = site_url + info[1]
    session = info[0]
    row = scraper_utils.initialize_row()
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    row.date_collected = datetime.now()
    
    row.session = session
    table = soup.find('div', 'col-sm-6 col-md-6')
    headers = table.find_all('div', {'class': 'col-md-4'})
    rows = table.find_all('div', class_ = 'col-md-8')
    
    for index, header in enumerate(headers):
        if header.text == 'Bill Number:':
            row.bill_name = rows[index].text.replace('PDF', '').strip()
            pdf_link = site_url + rows[index].find('a')['href']
            response = requests.get(pdf_link, stream = True ) 
            try:
                pdf =  pdfplumber.open(io.BytesIO(response.content)) 
                page = pdf.pages[ 0 ] 
                row.bill_text = page.extract_text()
            except Exception:
                traceback.print_exc()
                print(url)
                print(pdf_link)


        if header.text == 'Status:':
            row.current_status = rows[index].text.strip()
        if header.text == 'Originating Chamber:':
            row.chamber_origin = rows[index].text.strip()
        if header.text == 'Lead Sponsor:':
            sponsor = rows[index].text.strip()
            if len(sponsor.split(' ')) == 1 or bool(re.match('\w\.', sponsor)): # if lead sponsor is a person
                row.principal_sponsor = sponsor
                try:
                    legislator_source_url = rows[index].find('a')['href']
                    row.principal_sponsor_id = get_gov_id(legislator_source_url)
                except:
                    print('legislator not found, may be historical bill')

            else: # sponsor is a committee
                row.principal_sponsor = sponsor

        if header.text == 'Introduction Date:':
            row.date_introduced = rows[index].text.split('\xa0')[0].strip()
        if header.text == 'Other Primary Sponsor:':
            row.sponsors.append(rows[index].text.strip())
            try:
                legislator_source_urls = rows[index].find_all('a')
                for source_url in legislator_source_urls:
                    row.sponsors_id.append(get_gov_id(source_url['href']))
            except:
                print(f'legislator not found, may be historical bill at {url}')
        if header.text == 'CoSponsors:':
            row.cosponsors.append(rows[index].text.strip())
            try:
                legislator_source_urls = rows[index].find_all('a')
                for source_url in legislator_source_urls:
                    row.cosponsors_id.append(get_gov_id(source_url['href']))
            except:
                print(f'legislator not found, may be historical bill at {url}')

    h1 = soup.find('h1').text.split('-')
    row.bill_description = h1[1].title()
    if 'B' in h1[0]:
        row.bill_type = 'Bill'
    elif 'R' in h1[0]:
        row.bill_type = 'Resolution'
    else:
        raise Exception(f"Bill type not recognized: {url}")

    try:
        grid = soup.find('div', {'role':'grid', 'aria-colcount': 4})
        chambers = grid.find_all('div', {'class':'col-md-2', 'aria-colindex':1})[1:]
        date = grid.find_all('div', class_='col-md-3')[1:]
    
        description = grid.find_all('div', class_='col-sm-5 col-md-5')[1:]
        for index, chamber in enumerate(chambers):

            row.actions.append({'date':date[index].text.split('\xa0')[0].strip(), 'action_by':chamber.text.strip(), 'description': description[index].text.strip()})

        for desc in description:
            desc = desc.text
            text = desc.split('referred to')
            if len(text) == 2:
                committees = text[1].replace('the Committee on', '')
                try:
                    chamber = committees.split('-')[1]
                except IndexError:
                    chamber = row.chamber_origin
                committees = committees.split('-')[0].split(',')
                for c in committees:
                    row.committees.append({'chamber': chamber.title().strip(), 'committee':c.strip().title() })
                    
        votes = grid.find_all('a')


        for vote in votes:
            row_index = int(vote.parent.parent['aria-rowindex']) - 2
            desc = description[row_index].text.strip()
            timestamp = date[row_index].text.strip().split('\xa0')[0]
            chamber = chambers[row_index].text.strip()

            row.votes.append(get_votes(site_url + vote['href'], timestamp, desc, chamber))

    except AttributeError as e:
        traceback.print_exc()
        print(f'Bill at {url} has no history')


    row.source_id = row.session + '_' + row.bill_name 
    row.goverlytics_id = 'AR_' + row.session + '_' + row.bill_name
    
    

    return row
    
    # Depending on the data you're able to collect, the legislation scraper may be more involved
    # Than the legislator scraper. For one, you will need to create the goverlytics_id. The
    # goverlytics_id is composed of the state, session, and bill_name, The goverlytics_id can be
    # created like so:
    # goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    # row.goverlytics_id = goverlytics_id

    # Once you have the goverlytics_id, you can create the url:
    # row.url = f'/us/{state_abbreviation}/legislation/{goverlytics_id}'

    # The sponsor and cosponsor ID's are where things can get complicated, depending on how
    # much and what kind of data the legislation page has on the (co)sponsors. The
    # legislator_id's are pulled from the legislator database table, so you must be able to
    # uniquely identify each (co)sponsor... using just a last name, for instance, is not
    # sufficient since often more than one legislator will have the same last name. If you
    # have a unique identifier such as the (co)sponsor's state_url or state_member_id, use
    # that. Otherwise, you will have to use some combination of the data available to
    # identify. Using a first and last name may be sufficient.

    # To get the ids, first get the identifying fields, then pass them into the
    # get_legislator_id() function:
    # row.principal_sponsor_id = scraper_utils.get_legislator_id(state_url=legislator_state_url)
    # The get_legislator_id function takes in any number of arguments, where the key is
    # the column in the legislator table you want to search, and the value is the value
    # you want to search that column for. So having:
    # name_first = 'Joe'
    # name_last = 'Jimbo'
    # row.principal_sponsor_id = get_legislator_id(name_first=name_first, name_last=name_last)
    # Will search the legislator table for the legislator with the first and last name Joe Jimbo.
    # Note that the value passed in must match exactly the value you are searching for, including
    # case and diacritics.

    # In the past, I've typically seen legislators with the same last name denoted with some sort
    # of identifier, typically either their first initial or party. Eg: A. Smith, or (R) Smith.
    # If this is the case, scraper_utils has a function that lets you search for a legislator
    # based on these identifiers. You can also pass in the name of the column you would like to
    # retrieve the results from, along with any additional search parameters:
    # fname_initial = 'A.'
    # name_last = 'Smith'
    # fname_initial = fname_initial.upper().replace('.', '') # Be sure to clean up the initial as necessary!
    # You can also search by multiple letters, say 'Ja' if you were searching for 'Jason'
    # goverlytics_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', fname_initial, name_last=name_last)
    # The above retrieves the goverlytics_id for the person with the first name initial "A" and
    # the last name "Smith".

    # Searching by party is similar:
    # party = '(R)'
    # name_last = 'Smith'
    # party = party[1] # Cleaning step; Grabs the 'R'
    # goverlytics_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'party', party, name_last=name_last)

    # Other than that, you can replace this statement with the rest of your scraper logic.


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    # Get the session codes
    urls = get_urls_session()
    # Get all bill urls
    urls = get_urls_bills(urls)


    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislation_data_into_db(data)

    print('Complete!')

# save_file(get_urls_session(), 'step1.txt')
# save_file(get_urls_bills(open_file('step1.txt')), 'step2.txt')

# urls = open_file('step2.txt')
# for url in urls:
#     scrape(url)


