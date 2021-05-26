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
database_table_name = 'us_la_legislators'
country = 'US'

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

base_url = 'https://house.louisiana.gov'
wiki_url = 'https://en.wikipedia.org/'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_rep_urls():
    """
    collect all urls of house representatives' personal page.
    return: a list of urls
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
    scrape datas from house rep's personal page.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.

    param: take a url string
    return: return a dict that represent a row of info of a rep.
    '''

    row = scraper_utils.initialize_row()

    row.source_url = url

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    row.role = 'Representative'

    get_info_from_scraped_gov_url(row, soup)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


def get_info_from_scraped_gov_url(row, soup):
    """
    collect info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    get_name(row, soup)
    get_party(row, soup)
    get_district(row, soup)
    get_address(row, soup)
    get_phone_number(row, soup)
    get_email(row, soup)
    get_committees(row, soup)
    get_years_active(row, soup)
    get_occupation(row, soup)


def get_occupation(row, soup):
    """
        collect occupation info from personal page to fill the row. also check roles.

        param: take the row dict and soup of the webpage
    """
    #occupation

    try:
        occupation = []
        occ = soup.find('span', {'id': re.compile("body_FormView4_OCCUPATIONLabel")}).text
        occupation.append(occ)
        row.occupation = occupation
    except Exception:
        pass


def get_committees(row, soup):
    """
    collect committees info from personal page to fill the row. also check roles.

    param: take the row dict and soup of the webpage
    """
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


def get_email(row, soup):
    """
    collect email info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    # email
    email = soup.find('span', {'id': re.compile("body_FormView6_EMAILADDRESSPUBLICLabel")}).text
    row.email = email


def get_years_active(row, soup):
    """
    collect year elected info from personal page to fill the row. convert that into a years active list.

    param: take the row dict and soup of the webpage
    """
    # convert years elected to years active
    year_elected = soup.find('span', {'id': re.compile("body_FormView4_YEARELECTEDLabel")}).text
    if year_elected != "":
        try:
            years_active = list(range(int(year_elected), 2022))
            row.years_active = list(range(int(year_elected), 2022))
        except Exception:
            pass


def get_phone_number(row, soup):
    """
    collect phone number info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    # phone number
    raw_phone_number = soup.find('span', {'id': re.compile("body_FormView3_DISTRICTOFFICEPHONELabel")}).text
    phone_number = raw_phone_number.replace("(", "").replace(") ", "-")
    phone_number = [{'office': 'district office', 'number': phone_number}]
    row.phone_numbers = phone_number


def get_address(row, soup):
    """
    collect address info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    # address
    address = soup.find('span', {'id': re.compile("body_FormView3_OFFICEADDRESS2Label")}).text
    address = [{'location': 'district office', 'address': address}]
    row.addresses = address


def get_district(row, soup):
    """
    collect district info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    # district
    district = soup.find('span', {'id': re.compile("body_FormView5_DISTRICTNUMBERLabel")}).text
    row.district = district


def get_party(row, soup):
    """
    collect party info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    # party
    party = soup.find('span', {'id': re.compile("body_FormView5_PARTYAFFILIATIONLabel")}).text
    row.party_id = scraper_utils.get_party_id(party)
    row.party = party


def get_name(row, soup):
    """
    collect name info from personal page to fill the row.
    parses the name with name parser to determine first, middle and last name.

    param: take the row dict and soup of the webpage
    """
    # name
    name_full = soup.find('span', {'id': re.compile("body_FormView5_FULLNAMELabel")}).text
    if "Jonathan Goudeau, I" not in name_full:
        hn = HumanName(name_full)
        row.name_full = name_full
        row.name_last = hn.last
        row.name_first = hn.first
        row.name_middle = hn.middle
        row.name_suffix = hn.suffix
    elif "Jonathan Goudeau, I" in name_full:
        row.name_full = name_full
        row.name_last = "Goudeau"
        row.name_first = "Jonathan"
        row.name_suffix = "I"


def get_senate_urls():
    """
    collect all urls of senates' personal page

    return: a list of urls
    """
    urls = []
    # Logic goes here! Some sample code:
    base_url = 'https://senate.la.gov/'
    path = 'Senators_FullInfo'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

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
    scrape datas from house rep's personal page.

    Do not worry about collecting the goverlytics_id, date_collected, country, country_id,
    state, and state_id values, as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.

    param: take a url string
    return: return a dict that represent a row of info of a rep.
    '''

    row = scraper_utils.initialize_row()

    row.source_url = url

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    row.role = 'Senator'

    get_info_from_scraped_gov_url(row, soup)

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

    param: taks the wiki path of legislators wikipage
    return: a list of urls
    """

    print("If the tqdm bar seems stucked at 100%, please wait a bit longer, Thanks!")
    urls = []
    scrape_url = wiki_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'lxml')
    content_table = soup.find('table', {'class': 'wikitable sortable'})
    rows = content_table.find('tbody').find_all('tr')

    pbar = tqdm(range(1, len(rows)))
    for row in pbar:
        try:
            legislator_wikipage = rows[row].find_all('td')[1].find('a')
            href = legislator_wikipage.get('href')
            if '/wiki' in href:
                link = wiki_url + href
                district = legislator_wikipage.parent.parent.find('td').text.replace('\n','')
                urls.append([district, link])
        except Exception:
            pass
        scraper_utils.crawl_delay(crawl_delay)

    return urls


def scrape_wiki_site(wiki_urls, rows):
    """
    Take partially filled sorted rows and collect any other missing information from wikipedia (birthday, years served,
    etc).

    param: a list of wiki urls and list of legislator info row dicts
    return: a list of legislator info row dicts
    """

    for url in wiki_urls:
        wiki_info = scraper_utils.scrape_wiki_bio(url[1])
        url.append(wiki_info)

    for row in rows:
        for url in wiki_urls:
            if row.district == url[0] and row.name_last == url[2].get('name_last'):
                try:
                    row.education = url[2].get('education')
                    row.occupation = url[2].get('occupation')
                    row.birthday = url[2].get('birthday')
                    row.most_recent_term_id = url[2].get('most_recent_term_id')
                except Exception:
                    pass

    return rows


if __name__ == '__main__':

    sen_urls = get_senate_urls()
    with Pool() as pool:
        sen_partially_filled_rows = pool.map(scrape_senate, sen_urls)

    sen_wiki = get_wiki_urls('wiki/Louisiana_State_Senate')
    # pprint(sen_wiki)
    sen_data = scrape_wiki_site(sen_wiki, sen_partially_filled_rows)
    pprint(sen_data)

    rep_urls = get_rep_urls()
    with Pool() as pool:
        rep_partially_filled_rows = pool.map(scrape_rep, rep_urls)
    rep_wiki = get_wiki_urls('wiki/Louisiana_House_of_Representatives')
    rep_data = scrape_wiki_site(rep_wiki, rep_partially_filled_rows)
    pprint(rep_data)

    data = rep_data + sen_data
    scraper_utils.write_data(data)

    print('Complete!')
