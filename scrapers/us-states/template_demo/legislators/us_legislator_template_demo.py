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

from scraper_utils import ScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))
write_to_database = configParser.getboolean('miscConfig', 'write_to_database')
print_max_recods = configParser.getboolean('miscConfig', 'print_max_recods')
max_number_of_records_to_print = int(configParser.get('miscConfig', 'max_number_of_records_to_print'))
get_urls = configParser.getboolean('miscConfig', 'get_urls')
scrape_data = configParser.getboolean('miscConfig', 'scrape_data')
print_collected_urls = configParser.getboolean('miscConfig', 'print_collected_urls')

#Initialize database
db_user = str(configParser.get('databaseConfig', 'db_user'))
db_pass = str(configParser.get('databaseConfig', 'db_pass'))
db_host = str(configParser.get('databaseConfig', 'db_host'))
db_name = str(configParser.get('databaseConfig', 'db_name'))

Database.initialise(database=db_name, host=db_host, user=db_user, password=db_pass)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here!
    base_url = 'https://www.azleg.gov'
    path = '/memberroster/'
    scrape_url = base_url + path
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table', {'id': 'HouseRoster'})

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for tr in table.findAll('tr')[1:11]:
        a = tr.find('a', {'class':'roster-tooltip'})
        urls.append(a['href'])
    
    return urls


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Refer to the data dictionary if
    unsure.
    '''
    
    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row['state_url'] = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:
    
    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.
    party = 'Republican' 
    row['party_id'] = scraper_utils.get_party_id(party) 
    row['party'] = party

    # Other than that, you can replace this statement with the rest of your scraper logic.

    return row

if __name__ == '__main__':

    scraper_utils = ScraperUtils(state_abbreviation, database_table_name, country)

    if get_urls:
        urls = get_urls()
        if print_collected_urls:
            pprint(urls)

    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can iterate through the URLs individually, which is slower:

    if scrape_data:
        data = [scrape(url) for url in urls]

        with Pool() as pool:
            data = pool.map(scrape, urls)
            
        if write_to_database:
            scraper_utils.insert_legislator_data_into_db(data)
        if print_max_recods:
            for d in data[:max_number_of_records_to_print]:
                pprint(d)
