
import sys
import os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
import re
import numpy as np
from nameparser import HumanName
from multiprocessing import Pool
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
import time
from io import StringIO
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

state_abbreviation = 'KS'
database_table_name = 'us_ks_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'http://www.kslegislature.org'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():

    urls = []
    # Url we are scraping: http://www.kslegislature.org/li/b2021_22/chamber/senate/roster/
    path_senate = '/li/chamber/senate/roster/'
    path_house = '/li/chamber/house/roster/'

    # getting urls for senate
    scrape_url = base_url + path_senate
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'class': 'bottom'})
    items = table.find_all('tr')

    for tr in items[1:]:
        link = base_url + tr.find('a').get('href')
        # print(link)
        urls.append(link)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    # Collecting representatives urls
    scrape_url = base_url + path_house
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'class': 'bottom'})
    items = table.find_all('tr')

    for tr in items[1:]:
        link = base_url + tr.find('a').get('href')
        #  print(link)
        urls.append(link)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

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


def get_most_recent_term_id(soup, row):
    header_div = soup.find('div', {'id': 'logo2'})
    term_id = header_div.find('h5').text
    term_id = term_id.split(' ')[0].strip()

    row.most_recent_term_id = term_id


def find_party_and_district(main_div, row):
    party_block = main_div.find('h2').text
    party = party_block.split(' ')[3]

    row.party_id = scraper_utils.get_party_id(party)
    row.party = party

    district = party_block.split(' ')[1]
    row.district = district


def get_name_and_role(main_div, row):
    current_role = ""
    name_line = main_div.find('h1').text
    name_full = name_line

    if " - " in name_line:
        name_full = name_full[:name_full.index(" - ")]
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


def get_phone_numbers(capitol_office, home, business, row):
    phone_numbers = []
    try:
        number = capitol_office.text.split("Phone: ")[1].strip()
        phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', number)[0]
        phone_info = {'office': 'capitol office',
                      'number': phone_number.replace(' ', '-')}
        phone_numbers.append(phone_info)
    except:
        pass
    try:
        numbers = home.text.split("Phone: ")[1].strip()
        phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', numbers)[0]
        phone_info = {'office': 'home',
                      'number': phone_number.replace(' ', '-')}
        phone_numbers.append(phone_info)
    except Exception:
        pass

    try:
        numbers = business.text.split("Phone: ")[1].strip()
        phone_number = re.findall(r'[0-9]{3}[-, ][0-9]{3}[-, ][0-9]{4}', numbers)[0]
        phone_info = {'office': 'business',
                      'number': phone_number.replace(' ', '-')}
        phone_numbers.append(phone_info)
    except Exception:
        pass

    row.phone_numbers = phone_numbers


def get_email(capitol_office, row):
    capitol_email = capitol_office.a.text
    row.email = capitol_email


def get_address(capitol_office, row):
    addresses = []
    try:
        room_number = capitol_office.text.split("Room: ")[1].strip()
        room_number = room_number[:room_number.index('\n')]
    except:
        pass

    try:
        address = {'location': 'capitol office',
               'address': str(room_number) + ' - 300 SW 10th St. - Topeka, Kansas 66612'}
    except Exception as e:
        address = {'location': 'capitol office',
                   'address': '300 SW 10th St. - Topeka, Kansas 66612'}

    addresses.append(address)
    row.addresses = addresses


def get_occupation(business, row):
    jobs = []
    try:
        job = business.text.split("Occupation: ")[1].strip()

        if '\n' in job:
            job = job[:job.index('\n')]

        if '/' in job:
            jobs.append(job.split('/')[0])
            jobs.append(job.split('/')[1])
        else:
            jobs.append(job)
    except Exception:
        pass

    row.occupation = jobs


def get_years_active(contact_sidebar, row):
    terms = []
    years = []
    try:
        years_block = contact_sidebar.find_all('p')[4]
        years_text = re.findall(r'[0-9]{4}', years_block.text)
        for term in years_text:
            term = int(term)
            terms.append(term)
    except Exception:
        try:
            years_block = contact_sidebar.find_all('p')[3]
            years_text = re.findall(r'[0-9]{4}', years_block.text)
            for term in years_text:
                term = int(term)
                terms.append(term)
        except Exception:
            pass

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

    row.years_active = years


def get_committees(main_div, row):
    # get committees
    committees_list = []
    committee_leadership = main_div.find('tbody', {'id': 'commoffice-tab-1'})
    committee_member = main_div.find('tbody', {'id': 'comm-tab-1'})
    try:
        rows = committee_leadership.find_all('tr')
        for r in rows:
            row_details = r.find_all('td')
            role = row_details[0].text.strip()
            committee = row_details[1].text
            committee = committee[:committee.index(" -")].replace('\n', '')
            committee_detail = {"role": role, "committee": committee}
            committees_list.append(committee_detail)

    except Exception:
        pass
    try:
        member_row = committee_member.find_all('tr')
        for r in member_row:
            row_details = r.find_all('td')
            committee = row_details[0].text
            committee = committee[:committee.index("-")].strip()
            committee = committee.replace('\n', '')
            committee_detail = {"role": "member", "committee": committee}
            committees_list.append(committee_detail)
    except Exception:
        pass

    row.committees = committees_list


def get_areas_served(big_df_data):

    url = "http://www.kslegislature.org/li/b2021_22/members/csv/"
    members_data = requests.get(url)
    m_data = StringIO(members_data.text)
    df = pd.read_csv(m_data)[
        ['County', 'Firstname', 'Lastname']]
    for i in df.index:

        areas = []
        firstname = df.loc[i, "Firstname"]
        lastname = df.loc[i, "Lastname"]
        area = str(df.loc[i, "County"])
        areas.append(str(area))
        big_df_data.loc[(big_df_data['name_first'] == firstname) & (big_df_data['name_last'] == lastname),
                        'areas_served'] = pd.Series([areas]).values

    return big_df_data


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Kansas_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Kansas_State_Senate"

    if row.role == "Representative":
        try:
            uClient = uReq(wikipage_reps)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()
                party = party.replace('\n', '')
                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    if row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
        except Exception as e:
            print(e)
    if row.role == "Senator":

        try:
            uClient = uReq(wikipage_senate)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()

                if party == "Democratic":
                    party = "Democrat"

                if row.party == party.strip() and row.name_last in name.strip():
                    row.wiki_url = name_td.a['href']
                    break
        except Exception as e:
            print(e)
            pass


def scrape(url):

    row = scraper_utils.initialize_row()

    row.source_url = url

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    # getting the main part of the page
    main_div = soup.find('div', {'id': 'main'})

    # getting the sidebar data
    contact_sidebar = soup.find('div', {'id': 'sidebar'})

    capitol_office = contact_sidebar.find_all('p')[0]
    try:
        home = contact_sidebar.find_all('p')[1]
    except Exception:
        home = None
    try:
        business = contact_sidebar.find_all('p')[2]
    except Exception:
        business = None

    # calling data collection functions
    get_most_recent_term_id(soup, row)
    find_party_and_district(main_div, row)
    get_name_and_role(main_div, row)
    get_phone_numbers(capitol_office, home, business, row)
    get_email(capitol_office, row)
    get_address(capitol_office, row)
    get_occupation(business, row)
    get_years_active(contact_sidebar, row)
    get_committees(main_div, row)
    get_wiki_url(row)

    print(row.wiki_url)
    print(row.source_url + "\n")

    gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
    if not gender:
        gender = 'O'
    row.gender = gender
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    start = time.time()
    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) since the crawl delay is very large (ie: {crawl_delay} seconds). If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    print('Scraping data...')
    #data = scrape('http://www.kslegislature.org/li/b2021_22/members/rep_sawyer_tom_1/')
    data = [scrape(url) for url in urls]

    # with Pool() as pool:
    #     data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)
    leg_df = leg_df.drop(columns="birthday")
    leg_df = leg_df.drop(columns="education")


    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Kansas_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Kansas_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'education', 'name_first', 'name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    big_df['wiki_url'] = big_df['wiki_url'].replace({np.nan: None})

    print('Scraping complete')

    vacant_index = big_df.index[big_df['wiki_url'] == ''].tolist()
    for index in vacant_index:
        print(index)
        print(big_df.index[index])
        big_df = big_df.drop(big_df.index[index])

    vacant_index = big_df.index[big_df['wiki_url'] == None].tolist()
    for index in vacant_index:
        print(index)
        print(big_df.index[index])
        big_df = big_df.drop(big_df.index[index])

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')
