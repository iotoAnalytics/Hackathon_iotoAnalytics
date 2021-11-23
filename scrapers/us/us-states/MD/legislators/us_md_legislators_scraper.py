'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys, os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
from urllib.request import urlopen as uReq
import boto3
import pandas as pd
from tqdm import tqdm
from unidecode import unidecode
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = 'MD'
database_table_name = 'us_md_legislators'
country = 'US'

scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

base_url = 'https://mgaleg.maryland.gov'
wiki_url = 'https://en.wikipedia.org'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_rep_urls():
    """
    collect all urls of house representatives' personal page.
    return: a list of urls
    """
    urls = []
    # Logic goes here! Some sample code:
    path = '/mgawebsite/Members/Index/house'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # urls = {base_url + prod_path['href'] for prod_path in soup.findAll('a', {'href': re.compile("H_Reps/members")})}

    my_div = soup.find('div', {'id': 'myDIV'})
    legislator_list = my_div.find_all('div', {'class': 'col-5 text-left'})
    for item in legislator_list:
        try:
            # name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
            # if "Vacant" not in name:
            #     # leave out the vacant seat
            path = item.find('a', {'href': re.compile("/Members/Details")})
            scrape_url = base_url + path['href']
            urls.append(scrape_url)
        except:
            pass
    pprint(urls)
    return urls


def get_senate_urls():
    """
    collect all urls of senates' personal page

    return: a list of urls
    """
    urls = []
    # Logic goes here! Some sample code:
    path = '/mgawebsite/Members/Index/senate'
    scrape_url = base_url + path

    # request and soup
    page = requests.get(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    my_div = soup.find('div', {'id': 'myDIV'})
    legislator_list = my_div.find_all('div', {'class': 'col-5 text-left'})
    for item in legislator_list:
        # name = item.find('span', {'id': re.compile("body_ListView1_LASTFIRSTLabel")}).text
        # if "Vacant" not in name:
        #     # leave out the vacant seat
        path = item.find('a', {'href': re.compile("/Members/Details")})
        scrape_url = base_url + path['href']
        urls.append(scrape_url)

    pprint(urls)
    return urls


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("table")
    rows = tables[3].findAll("tr")

    for person in rows[1:]:
        info = person.findAll("td")
        try:
            biolink = info[1].a["href"]

            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


def get_wiki_url(row):
    wikipage_reps = "https://ballotpedia.org/Maryland_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Maryland_State_Senate"

    if row.role == "Representative":
        uClient = uReq(wikipage_reps)
    elif row.role == "Senator":
        uClient = uReq(wikipage_senate)

    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    table = page_soup.find("table", {"id": 'officeholder-table'})
    rows = table.findAll("tr")

    for person in rows[1:]:
        tds = person.findAll("td")
        name_td = tds[1]
        name = name_td.text
        name = name.replace('\n', '')
        name = HumanName(name)

        district_td = tds[0]
        district = district_td.text
        district_num = re.search(r'\d+', district).group().strip()

        if unidecode(name.last) == unidecode(row.name_last) and district_num == row.district:
            link = name_td.a['href']
            print(link)
            return link


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
    try:
        row.wiki_url = get_wiki_url(row)
    except:
        pass
    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender
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
    get_area_served(row, soup)
    get_district(row, soup)
    get_address(row, soup)
    get_phone_number(row, soup)
    get_email(row, soup)
    get_committees(row, soup)
    # get_years_active(row, soup)
    # get_occupation(row, soup)


def get_committees(row, soup):
    """
    collect committees info from personal page to fill the row. also check roles.

    param: take the row dict and soup of the webpage
    """
    # committees
    try:
        committees = []
        committees_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(6) > dl > dd")
        # print(committees_html)
        for i in committees_html:
            committees_text = i.get_text().replace("\n", "").split("\r")
        committees_text.pop(0)
        # print(committees_text)
        for committee in committees_text:
            if '(Chair)' in committee:
                committee_name = committee.replace(' (Chair)', '')
                committee_dict = {'role': 'chair', 'committee': committee_name.replace(" Committee", "").lower()}
                committees.append(committee_dict)
            elif '(Vice Chair)' in committee:
                committee_name = committee.replace(' (Vice Chair)', '')
                committee_dict = {'role': 'vice chair', 'committee': committee_name.replace(" Committee", "").lower()}
                committees.append(committee_dict)
            else:
                committee_dict = {'role': 'member', 'committee': committee.replace(" Committee", "").lower()}
                committees.append(committee_dict)
        row.committees = committees
    except Exception:
        pass


def get_email(row, soup):
    """
    collect email info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    try:
        email_tag = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(12) > a:nth-child(1)")
        for i in email_tag:
            email = i.text
        row.email = email
    except Exception:
        pass


# def get_years_active(row, soup):
#     """
#     collect year elected info from personal page to fill the row. convert that into a years active list.
#
#     param: take the row dict and soup of the webpage
#     """
#     # convert years elected to years active
#     year_elected = soup.find('span', {'id': re.compile("body_FormView4_YEARELECTEDLabel")}).text
#     if year_elected != "":
#         try:
#             years_active = list(range(int(year_elected), 2022))
#             row.years_active = list(range(int(year_elected), 2022))
#         except Exception:
#             pass


def get_phone_number(row, soup):
    """
    collect phone number info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    try:
        phone_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(8) > dl > dd:nth-child(2)")
        # print(phone_html)
        phone_str = phone_html
        for i in phone_html:
            phone_text_list = i.get_text().replace("\n", "").split("\r")
            phone_text = phone_text_list[1].strip()
        # print(phone_text)

        phone1 = re.findall('\d{3}-\d{3}-\d{4}', phone_text)[0]
        phone2 = re.findall('\d{3}-\d{3}-\d{4}', phone_text)[1]
        # print(phone1)
        # print(phone2)
        phone_numbers = [{'office': 'capitol office phone 1', 'number': phone1},
                        {'office': 'capitol office phone 2', 'number': phone2}]
        row.phone_numbers = phone_numbers

    except Exception:
        pass


def get_address(row, soup):
    """
    collect address info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    try:
        address_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(8) > dl > dd:nth-child(1)")
        # print(address_html)
        for i in address_html:
            address_text = i.get_text().replace("\n", "").split("\r")
        address_text.pop(0)
        # print(address_text)
        address_str = ", ".join(address_text).replace("  ", "")
        # print(address_str)
        capitol_address = address_str.replace(", ,", ",")
        # print(capitol_address[:-2])
        address = [{'location': 'capitol office', 'address': capitol_address[:-2]}]
        row.addresses = address
    except Exception:
        pass


def get_district(row, soup):
    """
    collect district info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    try:
        district_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(2)")
        # print(district_html)
        for i in district_html:
            district = i.get_text()
        # print(district)
        row.district = district
    except Exception:
        pass


def get_area_served(row, soup):
    """
    collect representing parish as area served.

    """
    try:
        area_served_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(4)")
        # print(area_served_html)
        for i in area_served_html:
            area_served = i.get_text()
        # print(area_served)
        row.areas_served = [area_served]
    except Exception:
        pass


def get_party(row, soup):
    """
    collect party info from personal page to fill the row.

    param: take the row dict and soup of the webpage
    """
    try:
        party_html = soup.select("#divMain > div > div.col.details-content-area > dl > dd:nth-child(14)")
        # print(party_html)
        for i in party_html:
            party = i.get_text()
        # print(party)
        row.party = party
        row.party_id = scraper_utils.get_party_id(party)
    except Exception:
        pass


def get_name(row, soup):
    """
    collect name info from personal page to fill the row.
    parses the name with name parser to determine first, middle and last name.

    param: take the row dict and soup of the webpage
    """
    name_html = soup.select("body > div.container-fluid.zero-out-padding-horizontal > div > div:nth-child(1) > div.col-sm-8.col-md-9.col-lg-10 > div > div:nth-child(1) > div > h2")
    # print(name_html)
    for i in name_html:
        name = i.get_text().replace("Senator ", "").replace("Delegate ", "")
    # print(name)
    hn = HumanName(name)
    row.name_full = name
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_middle = hn.middle
    row.name_suffix = hn.suffix


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
    content_table = soup.find('table', {'class': 'sortable wikitable'})
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

    param: a list of wiki urls and list of legislators info row dicts
    return: a list of legislators info row dicts
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
                    row.years_active = url[2].get('years_active')
                except Exception:
                    pass

    return rows


if __name__ == '__main__':

    sen_urls = get_senate_urls()
    with Pool() as pool:
        sen_partially_filled_rows = pool.map(scrape_senate, sen_urls)

    sen_wiki = get_wiki_urls('/wiki/Maryland_Senate')
    # pprint(sen_wiki)
    sen_data = scrape_wiki_site(sen_wiki, sen_partially_filled_rows)
    pprint(sen_data)

    rep_urls = get_rep_urls()
    with Pool() as pool:
        rep_partially_filled_rows = pool.map(scrape_rep, rep_urls)
    rep_wiki = get_wiki_urls('/wiki/Maryland_House_of_Delegates')
    rep_data = scrape_wiki_site(rep_wiki, rep_partially_filled_rows)
    pprint(rep_data)

    data = rep_data + sen_data
    scraper_utils.write_data(data)

    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Maryland_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Maryland_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')

    # scrape_rep("https://mgaleg.maryland.gov/mgawebsite/Members/Details/amprey01")

    print('Complete!')
