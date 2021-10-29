from database import CursorFromConnectionFromPool
import sys
import os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
from scraper_utils import USStateLegislationScraperUtils
import re
import numpy as np
from nameparser import HumanName
from multiprocessing import Pool
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import time

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

state_abbreviation = 'MA'
database_table_name = 'us_ma_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

legislator_table_name = 'us_ma_legislators'
legislation_scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://malegislature.gov'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():

    urls = []

    path_senate = '/Legislators/Members/Senate'
    path_house = '/Legislators/Members/House'

    # getting urls for senate
    scrape_url = base_url + path_senate
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'id': 'legislatorTable'})
    items = table.find_all('tr')

    for tr in items[1:]:
        td = tr.find_all('td')[2]
        link = base_url + td.find('a').get('href')
        urls.append(link)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    # Collecting representatives urls
    scrape_url = base_url + path_house
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table', {'id': 'legislatorTable'})
    items = table.find_all('tr')

    for tr in items[1:]:
        td = tr.find_all('td')[2]
        link = base_url + td.find('a').get('href')
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
    try:
        term_title = soup.find('span', {'class': 'headNumber'}).text
        term_id = term_title.split(' ')[1].strip()
        term_id = re.findall(r'[0-9]', term_id)
        term_id = "".join(term_id)
        row.most_recent_term_id = term_id
    except Exception:
        pass


def find_party_and_district(soup, row):
    try:
        party_block = soup.find('span', {'class': 'subTitle'}).text
        party = party_block.split(' -')[0].strip()
        row.party = party
    except:
        pass
    try:
        row.party_id = scraper_utils.get_party_id(party)
    except Exception:
        pass
    areas_served = []
    try:
        district = party_block.split('- ')[1]
        row.district = district
        areas = district.split(', ')
        for area in areas:
            if 'and' in area:
                a = area.split(' and ')
                areas_served = areas_served + a
            else:
                areas_served.append(area)
        row.areas_served = areas_served

    except Exception:
        pass


def get_name_and_role(soup, row):
    name_block = soup.find('h1')
    try:
        role = name_block.find('span').text.strip()
        name_full = name_block.text.split(role)[1].strip()
        if "Democrat" in name_full:
            name_full = name_full.split('Democrat')[0].strip()
        else:
            name_full = name_full.split('Republican')[0].strip()

        row.role = role

        hn = HumanName(name_full)
        row.name_full = name_full
        row.name_last = hn.last
        row.name_first = hn.first
        row.name_middle = hn.middle
        row.name_suffix = hn.suffix
    except:
        pass


def get_phone_numbers(soup, row):
    phone_numbers = []
    try:
        contacts = soup.findAll('div', {'class': 'col-xs-12 col-sm-5'})
        for contact in contacts:
            location = contact.find('h4').text.strip()
            number = contact.find('div', {'class': 'col-xs-12 col-lg-9'}).text.strip()
            number = number.replace("(", "").replace(")", "").replace(" ", "-")
            try:
                number = number.split("-x")[0]
            except:
                pass
            number = number.strip()
            phone_number = {"office": location, "number": number}
            phone_numbers.append(phone_number)
        row.phone_numbers = phone_numbers
    except Exception:
        pass


def get_email(soup, row):
    try:
        email = soup.find('address', {'class': 'repEmail'}).text.strip()
        row.email = email
    except:
        pass


def get_address(soup, row):
    addresses = []
    try:
        contacts = soup.find('div', {'class': 'col-xs-12 col-sm-5'})
        address = contacts.find('a').text.strip()
        address = address.replace('  ', '').replace('\n', '').replace('\r', '')
        address = {'location': 'capitol office',
                   'address': address}
        addresses.append(address)
        row.addresses = addresses
    except:
        pass


def get_biography(url, row):
    bio_url = url + '/Biography'
    page = scraper_utils.request(bio_url)
    soup = BeautifulSoup(page.content, 'lxml')
    #print(soup.text)
    try:
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, soup.text)
    except:
        try:
            gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        except:
            gender = 'O'
    if not gender:
        gender = 'O'
    row.gender = gender
    get_occupation(soup, row)
    scraper_utils.crawl_delay(crawl_delay)


def get_occupation(soup, row):
    jobs = []
    bio_section = soup.find('div', {'class': 'active tab-pane customFade in'})
    try:
        occupation = bio_section.find('div', {'class': 'col-xs-12 col-sm-9'}).text
        occupation = occupation.replace(';', '/')
        occupations = occupation.split('/')
        for job in occupations:
            job = job.replace('\n', '')
            job = job.replace('.', '')
            job = job.strip()
            jobs.append(job)
        row.occupation = jobs
    except Exception:
        pass


def get_committees_page(url, row):
    c_url = url + '/Committees'
    page = scraper_utils.request(c_url)
    soup = BeautifulSoup(page.content, 'lxml')
    get_committees(soup, row)
    scraper_utils.crawl_delay(crawl_delay)


def get_committees(soup, row):
    committees_list = []
    role = "member"
    try:
        c_section = soup.find('div', {'class': 'membershipList'})
        comm_list = c_section.find_all('li')
        for comm in comm_list:
            committee = comm.text
            committee = committee.replace('\n', '')
            if "Chairperson" in committee:
                role = "chairperson"
                committee = committee.split(", ")[1]
            elif "Vice Chair" in committee:
                role = "vice chair"
                committee = committee.split(", ")[1]
            committee_detail = {"role": role, "committee": committee}
            committees_list.append(committee_detail)
    except:
        pass
    row.committees = committees_list


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Massachusetts_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Massachusetts_State_Senate"

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

                if party == "Democratic":
                    party = "Democrat"


                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
        except Exception as e:
            print(e)
    if "Senat" in row.role:
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

                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
        except Exception as e:
            print(e)
            pass


def scrape(url):
    print(url)
    row = scraper_utils.initialize_row()

    row.source_url = url

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    get_most_recent_term_id(soup, row)
    find_party_and_district(soup, row)
    get_name_and_role(soup, row)
    get_phone_numbers(soup, row)
    get_email(soup, row)
    get_address(soup, row)
    get_biography(url, row)
    get_committees_page(url, row)
    get_wiki_url(row)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    print(row)
    return row


if __name__ == '__main__':

    print(
        f'WARNING: This website may take awhile to scrape (about 5-10 minutes using multiprocessing) since the crawl delay is very large (ie: {crawl_delay} seconds). If you need to abort, press ctrl + c.')
    print('Collecting URLS...')
    urls = get_urls()
    print('URLs Collected.')

    print('Scraping data...')
    # data = scrape('https://malegislature.gov/Legislators/Profile/MJM1')
    with Pool() as pool:
        data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data)
    leg_df = leg_df.drop(columns="birthday")
    leg_df = leg_df.drop(columns="education")
    leg_df = leg_df.drop(columns="years_active")
    leg_df.drop(leg_df.index[leg_df['email'] == 'firstsuffolkandmiddlesex@masenate.gov'], inplace=True)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Massachusetts_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Massachusetts_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_wiki_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['birthday', 'years_active', 'education', 'name_first', 'name_last']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_first", "name_last"])

    isna = big_df['education'].isna()
    big_df.loc[isna, 'education'] = pd.Series([[]] * isna.sum()).values
    big_df['birthday'] = big_df['birthday'].replace({np.nan: None})
    # big_df['years_active'] = big_df['years_active'].replace({np.nan: None})
    isna = big_df['years_active'].isna()
    big_df.loc[isna, 'years_active'] = pd.Series([[]] * isna.sum()).values

    # dropping rows with vacant seat
    try:
        vacant_index = big_df.index[big_df['party'] == "Unenrolled"].tolist()
        for index in vacant_index:
            big_df = big_df.drop(big_df.index[index])
    except:
        pass
    try:
        email_index = big_df.index[big_df['email'] == 'firstsuffolkandmiddlesex@masenate.gov'].tolist()
        for index in email_index:
            big_df = big_df.drop(big_df.index[index])
    except:
        pass
    try:
        name_index = big_df.index[big_df['name_full'] == ''].tolist()
        for index in name_index:
            big_df = big_df.drop(big_df.index[index])
    except:
        pass


    print('Scraping complete')

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')
