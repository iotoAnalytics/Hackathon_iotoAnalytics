'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
import io
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from PyPDF2 import PdfFileReader
from scraper_utils import CAProvinceTerrLegislationScraperUtils, USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3


# Other import statements


prov_terr_abbreviation = 'NB'
database_table_name = 'ca_nb_legislation'
legislator_table_name = 'ca_nb_legislators'

scraper_utils = CAProvinceTerrLegislationScraperUtils(
    prov_terr_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://www1.gnb.ca/'
bills_url1 = base_url + '/legis/bill/index-e.asp?page=1&legi=60&num=1'
bills_url2 = base_url + '/legis/bill/index-e.asp?page=2&legi=60&num=1'
bills_url3 = base_url + '/legis/bill/index-e.asp?page=3&legi=60&num=1'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)
 

def get_urls(url):
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:

    path = '/test-sites/e-commerce/allinone'
    scrape_url = base_url + path
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')[9]
    rows = table.find_all('tr')
    for row in rows:
        url = row.find_all('td')[1]
        url = url.find('a').get('href')
        urls.append(url)
        print(url)
    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls


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
    url_base = 'https://www1.gnb.ca/legis/bill/'
    scrape_url = url_base + url
    row.source_url = scrape_url

    row.goverlytics_id = url

    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')[6]
    

    rows = table.find_all('tr')
    

    first_row = rows[0].find_all('td')
    title = first_row[0].getText()
    print(title)
    second_row = rows[2].find_all('td')
    
    legislature = second_row[1].getText()
    third_row = rows[3].find_all('td')
    fourth_row = rows[4].find_all('td')
    first_reading = fourth_row[1].getText()
    
    fifth_row = rows[5].find_all('td')
    sixth_row = rows[6].find_all('td')
    
    seventh_row = rows[7].find_all('td')
    
    pdf_link = seventh_row[1].find('a').get('href')
    pattern = ('=\d*')
    source_id = re.search(pattern,url)[0][1:]
    print (source_id)
    row.session = legislature
    row.source_id = source_id 
    row.date_introduced = first_reading
    row.goverlytics_id = f'{prov_terr_abbreviation.strip()}_{legislature.strip()}_{source_id.strip()}'
    print(pdf_link)

    
    r = requests.get(pdf_link)
    scraper_utils.crawl_delay(crawl_delay)
    f = io.BytesIO(r.content)
    
    reader = PdfFileReader(f)
    contents = reader.getPage(0).extractText().strip()
    row.bill_text = contents


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

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls(bills_url1)
    urls2 = get_urls(bills_url2)
    urls3 = get_urls(bills_url3)
    urls.append(urls2)
    urls.append(urls3)

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)

    print('Complete!')
