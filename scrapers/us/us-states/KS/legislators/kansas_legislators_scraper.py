'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import boto3
import re
import numpy as np
from nameparser import HumanName
from pprint import pprint
from multiprocessing import Pool
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
from urllib.request import Request
import time
from scraper_utils import USStateLegislatorScraperUtils
from tqdm import tqdm

state_abbreviation = 'KS'
database_table_name = 'test_us_ks_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'http://www.kslegislature.org'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []
    # Url we are scraping: http://www.kslegislature.org/li/b2021_22/chamber/senate/roster/
    path_senate = '/li/b2021_22/chamber/senate/roster/'
    path_house = '/li/b2021_22/chamber/house/roster/'

    # getting urls for senate
    scrape_url = base_url + path_senate
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'class': 'bottom'})
    items = table.find_all('tr')

    # We'll collect only the first 10 to keep things simple. Need to skip first record
    for tr in items[1:11]:
        link = base_url + tr.find('a').get('href')
        # print(link)
        urls.append(link)

    # Collecting representatives urls
    scrape_url = base_url + path_house
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'class': 'bottom'})
    items = table.find_all('tr')
    # scraping just 10 for testing
    for tr in items[1:11]:
        link = base_url + tr.find('a').get('href')
        #  print(link)
        urls.append(link)

    # Delay so we do not overburden servers
  #  scraper_utils.crawl_delay(crawl_delay)

    return urls


def find_reps_wiki(repLink):
    bio_lnks = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("tbody")
    people = tables[3].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        try:
            biolink = "https://en.wikipedia.org" + (info[1].a["href"])

            bio_lnks.append(biolink)
        except Exception:
            pass

    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def find_sens_wiki(repLink):
    bio_links = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = BeautifulSoup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[3].findAll("tr")
    for person in people[1:]:
        info = person.findAll("td")
        try:
            biolink = "https://en.wikipedia.org" + (info[2].a["href"])

            bio_links.append(biolink)
        except Exception:
            pass

    scraper_utils.crawl_delay(crawl_delay)
    return bio_links


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url
    # like so:
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    main_div = soup.find('div', {'id': 'main'})
    party_block = main_div.find('h2').text

    party = party_block.split(' ')[3]

    row.party_id = scraper_utils.get_party_id(party)
    row.party = party
    # print(party)

    # Get names and current roll (House or Senate)
    current_role = ""
    name_line = main_div.find('h1').text
    name_full = name_line

    if "-" in name_line:
        name_full = name_full[:name_full.index("-")]
    if "Senator" in name_full:
        name_full = name_full.replace('Senator ', '')
        current_role = "Senator"
    if "Representative" in name_full:
        current_role = "Representative"
        name_full = name_full.replace('Representative ', '')

    row.role = current_role
    

    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix
    # print(name_full)

    # Get district

    district = party_block.split(' ')[1]
    #  print(district)
    row.district = district

    # Get phone number
    phone_numbers = []
    contact_sidebar = soup.find('div', {'id': 'sidebar'})

    capitol_office = contact_sidebar.find_all('p')[0]
    home = contact_sidebar.find_all('p')[1]
    business = contact_sidebar.find_all('p')[2]

    number = capitol_office.text.split("Phone: ")[1].strip()
    phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', number)[0]
    phone_info = {'office': 'Capitol Office',
                  'number': phone_number}
    phone_numbers.append(phone_info)

    try:
        numbers = home.text.split("Phone: ")[1].strip()
        phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', numbers)[0]
        phone_info = {'office': 'Home',
                      'number': phone_number}
        phone_numbers.append(phone_info)
    except:
        pass

    try:
        numbers = business.text.split("Phone: ")[1].strip()
        phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', numbers)[0]
        phone_info = {'office': 'Business',
                      'number': phone_number}
        phone_numbers.append(phone_info)
    except:
        pass

    row.phone_numbers = phone_numbers
    #  print(phone_numbers)

    # get email
    capitol_email = capitol_office.a.text
    row.email = capitol_email
    #   print(capitol_email)

    # get occupation
    jobs = []
    job = business.text.split("Occupation: ")[1].strip()

    if '\n' in job:
        job = job[:job.index('\n')]

    if '/' in job:
        jobs.append(job.split('/')[0])
        jobs.append(job.split('/')[1])
    else:
        jobs.append(job)

    #  print(jobs)
    row.occupation = jobs

    # get years active
    terms = []
    years = []
    try:
        years_block = contact_sidebar.find_all('p')[4]
        years_text = re.findall(r'[0-9]{4}', years_block.text)
        for term in years_text:
            term = int(term)
            terms.append(term)
    #   print(terms)
    except:
        pass

    # Convert term length to actual years

    # Converting terms from the form year-year eg. 2012-2014
    if len(terms) > 1:
        j = 0
        k = -1
        for i in range(1, len(terms), 2):
            j += 2
            k += 2
            for year in range(terms[len(terms) - j], terms[len(terms) - k] + 1):
                years.append(year)

    # converting term which is current
    if len(terms) > 0:
        for year in range(terms[0], 2021 + 1):
            years.append(year)

    # print(years)

    row.years_active = years

    # get committees

    tabs_section = main_div.find_all('div', {'class':'tabs'})
    
    print(tabs_section)
    # # Delay so we do not overburden servers
   # scraper_utils.crawl_delay(crawl_delay)
    #
    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) since the crawl delay is very large (ie: {crawl_delay} seconds). If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    print('Scraping data...')

    with Pool() as pool:
        data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)
    leg_df = leg_df.drop(columns="birthday")
    leg_df = leg_df.drop(columns="education")

   
    wiki_rep_link = 'https://en.wikipedia.org/wiki/Kansas_House_of_Representatives'
    wiki_sen_link = 'https://en.wikipedia.org/wiki/Kansas_Senate'

    reps_wiki = find_reps_wiki(wiki_rep_link)

    sens_wiki = find_sens_wiki(wiki_sen_link)

    all_wiki_links = reps_wiki
    for sw in sens_wiki:
        all_wiki_links.append(sw)

    with Pool() as pool:

        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_first", "name_last"])
   
    big_df['education'] = big_df['education'].replace({np.nan: None})
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})

    print('Scraping complete')
 
    big_list_of_dicts = big_df.to_dict('records')
    #print(big_list_of_dicts)

    print('Writing data to database...')

    #scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran succesfully!')
