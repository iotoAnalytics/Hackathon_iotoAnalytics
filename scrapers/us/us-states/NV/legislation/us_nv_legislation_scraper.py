'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
from itertools import dropwhile
from random import seed
import sys
import os
import io
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pformat, pprint 
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from time import sleep
from datetime import datetime
from PyPDF2 import PdfFileReader
from tqdm import tqdm



# Other import statements


state_abbreviation = 'NV'
database_table_name = 'us_nv_legislation'
legislator_table_name = 'us_nv_legislator'


options = Options()
options.headless = False 
driver = webdriver.Chrome(executable_path=r'C:\Users\DCYUM\Downloads\chromedriver_win32\chromedriver.exe', options=options)
driver.switch_to.default_content()


scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)


base_url = 'https://www.leg.state.nv.us'
bills_url = 'https://www.leg.state.nv.us/App/NELIS/REL/81st2021/Bills/List'

# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

session_id = '2021'

def get_urls():
    driver.get(bills_url)
    driver.maximize_window()
    urls = []
    sleep(2)

    select = Select(driver.find_element_by_id('pageSizeVisible'))
    select.select_by_value('2147483647')
    sleep(3)
    session_id = driver.find_element_by_css_selector('#mainMenu > div.bg-main.navbar-dark.row.flex-no-wrap.justify-content-between.align-items-center.header-divider.mx-0.p-1 > div.col-sm-12.col-md-6.d-flex.justify-content-between > div.text-right.font-weight-bold.session-text')
    session_id = session_id.get_attribute('textContent')
    session_id = session_id[session_id.find("(")+1:session_id.find(")")]
  
    list_rows = driver.find_elements_by_css_selector('#billList > div.row > div > a')
    for row in list_rows :
        url = row.get_attribute('href')
        urls.append(url)
    
    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)
    driver.close()

    return urls


def get_history():
    actions = []
    action_list = driver.find_element_by_xpath("//*[contains(text(), 'Bill History')][1]/following-sibling::tbody")
    action_list = action_list.find_elements_by_css_selector('tr')
    first_introduced = None
    
    

    for act in action_list :
        
        coloumns = (act.find_elements_by_css_selector('td'))
        if first_introduced is None :
            first_introduced = coloumns[0].get_attribute('textContent')
        action = {
            'date': coloumns[0].get_attribute('textContent'),
            'action_by':coloumns[1].get_attribute('textContent'),
            'description':coloumns[2].get_attribute('textContent')
        }
        
        actions.insert(0,action)

    history = {
        'actions' : action,
        'introduced' : first_introduced 
    }
    return history

def get_votes_number(info):
    vote_number = info.find_element_by_css_selector("a").get_attribute('textContent')
    vote_number = vote_number.split(': ')[1]
    vote_number = int(vote_number)
    return(vote_number)

def get_voters(driver):
    driver.click()
    voter_list = []
    sleep(1)

    people_list = driver.find_elements_by_css_selector('ul > li > div > div')
    

    for voter in people_list:
        
        voter_string = voter.get_attribute('textContent')
        voter_string = re.sub("\s\s+", " ", voter_string)
        legislator = voter_string.rsplit("(",1)[0].strip()
        name = HumanName(legislator)
        goverlytics_id = None
    
        search_name = dict(name_last = name.last,name_first = name.first)
        goverlytics_id = scraper_utils.get_legislator_id(**search_name)
        if goverlytics_id is None:
            
            l_name = legislator.split(" ")[1].replace(',','')
            search_name = dict(name_last = l_name ,name_first = name.first)
            goverlytics_id = scraper_utils.get_legislator_id(**search_name)
            
        
        voter_info = {
            'goverlytics_id' : goverlytics_id,
            'legislator':legislator,
            'vote_text':voter_string[voter_string.find("(")+1:voter_string.find(")")]
        }
        voter_list.append(voter_info)
        
            

    return voter_list

def get_votes() :
    votes_page = driver.find_element_by_css_selector('#tabVotes > span.k-link')
    votes_page.click()
    sleep(1)
    vote_data = driver.find_element_by_id('billVoteWidget')
    vote_data = vote_data.find_elements_by_class_name("col-sm-12")
    votes = []
   
    for vote in vote_data :
        try:
            first_text = vote.find_element_by_css_selector('h2').get_attribute('textContent').split(' ')
            chamber = first_text[0]
            
            details = first_text[1].replace('(','').replace(')','')
          
            passed = 0
            first_section = vote.find_element_by_css_selector('ul')
            first_section = first_section.find_elements_by_css_selector('li > span')
            


            if 'Yes' in first_section[0].get_attribute('textContent'):
                passed = 1
            
            date = first_section[1].get_attribute('textContent')
      
            date_object = datetime.strptime(date,'%A, %B %d, %Y')
            date = date_object.date()
            specific_votes = vote.find_element_by_css_selector("div")
            specific_votes = specific_votes.find_elements_by_xpath('./div')
            
            total = get_votes_number(specific_votes[0])
            yeas = get_votes_number(specific_votes[1])
            nays = get_votes_number(specific_votes[2])
            nv = get_votes_number(specific_votes[4])
            absent = get_votes_number(specific_votes[5])
            people_votes = get_voters(specific_votes[0])
            



            vote_details = {
                'date' :date,
                'description':details,
                'yeas' :yeas,
                'nays' :nays,
                'nv' :nv,
                'absent':absent,
                'total':total,
                'passed': passed,
                'chamber':chamber,
                'votes':people_votes,
            }
        
            print("No vote data")
            votes.append(vote_details)
        except:
            print('No specific voting data')
    
    driver.find_element_by_id('tabOverview').click()
    return votes

def get_sponsors(list):
    id_list = []
    name_list = []
    for sponsor in list:
        target = sponsor.find_element_by_css_selector('a')
        sponsor_name = target.get_attribute('textContent').split(' ',1)[1]
        
        name_parts = sponsor_name.split()

        search_name = dict(name_last = name_parts[1],name_first = name_parts[0])
        goverlytics_id = scraper_utils.get_legislator_id(**search_name)
        
        id_list.append(goverlytics_id)
        name_list.append(sponsor_name)
    
    sponsor_info = {
            'ids' : id_list,
            'names' : name_list
        }
    
    return sponsor_info
        


def get_primary_sponsors():
    sponsor_url = driver.find_element_by_id('primarySponsors')
    sponsor_list = sponsor_url.find_elements_by_css_selector('ul > li')

    return get_sponsors(sponsor_list)

def get_cosponsors():
    cosponsor_url = driver.find_element_by_id('cosponsors')
    cosponsor_list = cosponsor_url.find_elements_by_css_selector('ul > li')

    return get_sponsors(cosponsor_list)

def get_bill_info() :
    bill_id = driver.find_element_by_css_selector('#bills-container > div > div.col-lg-9.pt-lg-4.main-content > div:nth-child(3) > div > h1').get_attribute('textContent')
    origin = None
    type = None
    if 'S' in bill_id :
        
        origin = "Senate"
    else:
        origin = "House"

    
    if 'B' in bill_id :
        
        type = "Bill"
    elif "R":
        type  = "Resoultion"
    


    bill_info = {
        'origin':origin,
        'type': type
    }
    
    return bill_info

def get_text():
    driver.find_element_by_css_selector('#tabText').click()
    sleep(1)
    pdf_link = driver.find_element_by_css_selector('#divText > div.d-md-none > div > div > ul > li:last-child > div > p > a').get_attribute('href')
    #divText > div.d-md-none > div > div > ul > li:nth-child(5) > div > p > a
    

    r = requests.get(pdf_link)
    f = io.BytesIO(r.content)

    reader = PdfFileReader(f)
    contents = reader.getPage(0).extractText().split('\n')
   
    
    
    return contents

def get_committes():
    committees = []
    main_committee = driver.find_element_by_css_selector('#divOverview > div > div:nth-child(4) > div.col > a').get_attribute('textContent')
    chamber = 'House'
    if 'Senate' in main_committee:
        chamber = 'Senate'
    
    
    comittee = {
        'chamber':chamber,
        'committee':main_committee.split(' ',3)[3]
    }

    committees.append(comittee)
    return None

def scrape(url):
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

    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    #goverlytics_id = f'{state_abbreviation}_{session_id}_{bill_name}'
    #row.goverlytics_id = goverlytics_id
    row.goverlytics_id = url
    row.session = session_id
    
    driver.get(url)
    driver.maximize_window()
    sleep(3)
    try :

        history = get_history()
        row.actions = history['actions']
        row.date_intoduced = history['introduced']
    except:
        print('No History')

    try :
        bill_info = get_bill_info()
        row.chamber_origin = bill_info['origin']
        row.bill_type = bill_info['type']
        row.bill_title = driver.find_element_by_id('title').get_attribute('textContent')
        row.bill_summary = driver.find_element_by_css_selector('#divOverview > div > div:nth-child(1) > div.col').text
    except:
        print('No info')

    #try:
    #    row.committees = get_committes
    #except:
    #    print('No committees')
    try:
        row.bill_text = get_text()
    except:
        print('No current text')
    try:
        row.votes = get_votes()
    except:
        print('No vote data')
    
    try:
        sponsors = get_primary_sponsors()
    
        row.sponsors = sponsors['names']
        row.sponsors_id = sponsors['ids']
    except:
        print('no sponsors')

    try:
        cosponsor = get_cosponsors()
        row.cosponsors = cosponsor['names']
        row.cosponsors_id = cosponsor['ids']
    except:
        print('no cosponsors')
    
    
    scraper_utils.crawl_delay(crawl_delay)

    return row



if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    #scrape('https://www.leg.state.nv.us/App/NELIS/REL/81st2021/Bill/8021/Overview')
    #data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = list(tqdm(pool.imap(scrape, urls)))

    # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)
    

    print('Complete!')
