'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path

#set path to current file directory
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]
sys.path.insert(0, str(p))

from legislation_scraper_utils import LegislationScraperUtils, LegislationRow
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

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
legislator_table_name = str(configParser.get('scraperConfig', 'legislator_table_name'))

scraper_utils = LegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)

def get_years():
    scrape_url = 'https://www.arkleg.state.ar.us/Bills/Search?ddBienniumSession=2015%2F2015R'
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    years = soup.find_all('label', class_='session')

    return years


def get_urls():

    years = ['z' + year['for'][7:] for year in get_years()]
    odd_years = list(set([year[1:5]  for year in years if int(year[1:5]) % 2 == 1]))
    
    for year in odd_years:
        years.append('b' + year)
    years = '%2C'.join(years)

    scrape_url = f'''https://www.arkleg.state.ar.us/Bills/Search?tbType=&ddBienniumSession=2015%2F2015R&bienniumAll=on&hdnSessions=on%2C{years}%2C&ddChamber=A&tbActNumber=&tbBillNumber=&ddSponsor=&ddCoSponsor=&tbAllWords=&tbExactPhrase=&tbOneWord=&tbWithoutWords=&ddExclusivity=Only
            '''
    header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(executable_path=r'C:\\Users\\Avery\\Downloads\\chromedriver_win32\\chromedriver.exe', options=options)

    driver.switch_to.default_content()
    driver.get(scrape_url)
    driver.maximize_window()
    link = driver.find_element_by_tag_name('a')
    print(link)
    



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
    row.state_url = url

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

    return row

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
#     scraper_utils.insert_legislation_data_into_db(data)

#     print('Complete!')

get_urls()
