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

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from tqdm import tqdm

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = 'LA'
database_table_name = 'us_la_legislators_test'
country = 'US'

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

base_url = 'https://house.louisiana.gov'
wiki_url = 'https://en.wikipedia.org/'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_rep_urls():
    """
    collect all urls of house representatives' personal page
    """
    urls = []
    # Logic goes here! Some sample code:
    base_url = 'https://house.louisiana.gov'
    path = '/H_Reps/H_Reps_FullInfo'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # urls = {base_url + prod_path['href'] for prod_path in soup.findAll('a', {'href': re.compile("H_Reps/members")})}

    legislator_list = soup.find_all('div', {'class': 'media-body'})
    for item in legislator_list:
        name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        if "Vacant" not in name:
            # leave out the vacant seat
            path = item.find('a', {'href': re.compile("H_Reps/members")})
            scrape_url = base_url + path['href']
            urls.append(scrape_url)

    return urls


def scrape_rep(url):
    '''
    Insert logic here to scrape_rep all URLs acquired in the get_urls() function.

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
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    row.role = 'Representative'

    set_info_from_scraped_gov_url(row, soup)

    # year_active: I have year_elected on the webpages, should I rely on that info alone and convert that into
    # years_active? or should I collect that elsewhere?

    # education: the webpage also have edu info but they are unstructured and unable to meet the dataDict requirements

    # Other than that, you can replace this statement with the rest of your scraper logic.

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


def set_info_from_scraped_gov_url(row, soup):
    set_name(row, soup)
    set_party(row, soup)
    set_district(row, soup)
    set_address(row, soup)
    set_phone_number(row, soup)
    set_email(row, soup)
    set_committees(row, soup)


def set_committees(row, soup):
    # committees
    committees = []
    committees_html = soup.find('span', {'id': re.compile("body_FormView1_COMMITTEEASSIGNMENTS2Label")})
    for committee in committees_html.stripped_strings:
        if 'Chairman' in committee:
            committee_name = committee.replace(', Chairman', '')
            committee_dict = {'role': 'chairman', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        elif 'Vice Chair' in committee:
            committee_name = committee.replace(', Vice Chair', '')
            committee_dict = {'role': 'vice chair', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        elif 'Ex Officio' in committee:
            committee_name = committee.replace(', Ex Officio', '')
            committee_dict = {'role': 'ex officio', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        elif 'Interim Member' in committee:
            committee_name = committee.replace(', Interim Member', '')
            committee_dict = {'role': 'interim member', 'committee': committee_name.lower()}
            committees.append(committee_dict)
        else:
            committee_dict = {'role': 'member', 'committee': committee.lower()}
            committees.append(committee_dict)
    row.committees = committees


def set_email(row, soup):
    # email
    email = soup.find('span', {'id': re.compile("body_FormView6_EMAILADDRESSPUBLICLabel")}).text
    row.email = email


def set_phone_number(row, soup):
    # phone number PROBLEM HERE
    raw_phone_number = soup.find('span', {'id': re.compile("body_FormView3_DISTRICTOFFICEPHONELabel")}).text
    phone_number = raw_phone_number.replace("(", "").replace(") ", "-")
    phone_number = [{'office': 'district office', 'number': phone_number}]
    row.phone_numbers = phone_number


def set_address(row, soup):
    # address
    address = soup.find('span', {'id': re.compile("body_FormView3_OFFICEADDRESS2Label")}).text
    address = [{'location': 'district office', 'address': address}]
    row.addresses = address


def set_district(row, soup):
    # district
    district = soup.find('span', {'id': re.compile("body_FormView5_DISTRICTNUMBERLabel")}).text
    row.district = district


def set_party(row, soup):
    # party
    party = soup.find('span', {'id': re.compile("body_FormView5_PARTYAFFILIATIONLabel")}).text
    row.party_id = scraper_utils.get_party_id(party)
    row.party = party


def set_name(row, soup):
    # name
    name_full = soup.find('span', {'id': re.compile("body_FormView5_FULLNAMELabel")}).text
    hn = HumanName(name_full)
    row.name_full = name_full
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix


def get_senate_urls():
    """
    collect all urls of senates' personal page
    """
    urls = []
    # Logic goes here! Some sample code:
    base_url = 'https://senate.la.gov/'
    path = 'Senators_FullInfo'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # urls = {base_url + prod_path['href'] for prod_path in soup.findAll('a', {'href': re.compile("H_Reps/members")})}

    legislator_list = soup.find_all('div', {'class': 'media-body'})
    for item in legislator_list:
        name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        if "Vacant" not in name:
            # leave out the vacant seat
            path = item.find('a', {'href': re.compile("smembers")})
            scrape_url = base_url + path['href']
            urls.append(scrape_url)

    pprint(urls)
    return urls


def scrape_senate(url):
    '''
    Insert logic here to scrape_senate all URLs acquired in the get_urls() function.

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
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    # The only thing to be wary of is collecting the party and party_id. You'll first have to collect
    # the party name from the website, then get the party_id from scraper_utils
    # This can be done like so:

    # Replace with your logic to collect party for legislator.
    # Must be full party name. Ie: Democrat, Republican, etc.

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    row.role = 'Senator'

    set_info_from_scraped_gov_url(row, soup)

    # year_active: I have year_elected on the webpages, should I rely on that info alone and convert that into
    # years_active? or should I collect that elsewhere?

    # education: the webpage also have edu info but they are unstructured and unable to meet the dataDict requirements

    # Other than that, you can replace this statement with the rest of your scraper logic.

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


def get_legislator_info_from_main_page(soup):
    """
    get name, district, party, address, number and email of legislators from main page.
    """
    info_list = []
    name_first = []
    name_middle = []
    name_last = []
    legislator_list = soup.find_all('div', {'class': 'media-body'})

    for item in legislator_list:
        name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        if "Vacant" not in name:
            name_full = name
            name_parser = HumanName(name)
            name_last = name_parser.last
            name_first = name_parser.first
            name_middle = name_parser.middle
            district = item.find('span', {'id': re.compile("body_ListView1_DISTRICTNUMBERLabel")}).text
            party = item.find('span', {'id': re.compile("body_ListView1_PARTYAFFILIATIONLabel")}).text
            address = item.find('span', {'id': re.compile("body_ListView1_OFFICEADDRESSLabel")}).text
            phone_number = item.find('span', {'id': re.compile("body_ListView1_DISTRICTOFFICEPHONELabel")}).text
            email = item.find('span', {'id': re.compile("body_ListView1_EMAILADDRESSPUBLICLabel")}).text

            info_dict = {
                'name_full': name_full,
                'name_last': name_last,
                'name_first': name_first,
                'name_middle': name_middle,
                'district': district,
                'party': party,
                'address': address,
                'phone_number': phone_number,
                'email': email
            }
            info_list.append(info_dict)

    return info_list


def get_wiki_urls(path):
    """
    Takes base URL of wikipedia, scrape senates/house member's page URLS.
    """

    urls = []
    scrape_url = wiki_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    content_table = soup.find('table', {'class': 'wikitable sortable'})
    rows = content_table.find('tbody').find_all('tr')

    pbar = tqdm(range(1, len(rows)))
    for row in pbar:
        try:
            href = rows[row].find_all('td')[1].find('a').get('href')
            if '/wiki' in href:
                link = wiki_url + href
                urls.append(link)
        except Exception:
            pass
        scraper_utils.crawl_delay(crawl_delay)

    return urls


def scrape_wiki_site(wiki_urls, sorted_rows):
    """
    Take partially filled sorted rows and collect any other missing information from wikipedia (birthday, years served,
    etc).
    """

    pbar = tqdm(range(len(sorted_rows)+1))
    for item in pbar:
        wiki_info = scraper_utils.scrape_wiki_bio(wiki_urls[item])
        sorted_rows[item].education = wiki_info['education']

        if wiki_info['birthday'] is not None:
            sorted_rows[item].birthday = str(wiki_info['birthday'])
        sorted_rows[item].years_active = wiki_info['years_active']
        sorted_rows[item].most_recent_term_id = wiki_info['most_recent_term_id']

    return sorted_rows


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape_rep:
    # html = get_main_page_html()
    # info_list = get_legislator_info_from_main_page(html)
    # pprint(info_list)

    # Next, we'll scrape_rep the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    #
    # # Once we collect the data, we'll write it to the database.
    # scraper_utils.insert_legislator_data_into_db(data)
    # rep_urls = get_rep_urls()
    # data = [scrape_rep(url) for url in rep_urls]
    # with Pool() as pool:
    #     data = pool.map(scrape_rep, rep_urls)

    sen_urls = get_senate_urls()
    with Pool() as pool:
        data = pool.map(scrape_senate, sen_urls)

    sen_partially_filled_sorted_rows = sorted(data, key=lambda row: (row.role, int(row.district)))
    # rep_wiki = get_wiki_urls('wiki/Louisiana_House_of_Representatives')
    sen_wiki = get_wiki_urls('wiki/Louisiana_State_Senate')
    # pprint(sen_wiki)
    sen_data = scrape_wiki_site(sen_wiki, sen_partially_filled_sorted_rows)
    pprint(sen_data)
    print('Complete!')
