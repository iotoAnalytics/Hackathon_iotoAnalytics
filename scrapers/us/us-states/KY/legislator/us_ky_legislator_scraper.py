import sys
import os
from pathlib import Path
from scraper_utils import USStateLegislatorScraperUtils
import re
from unidecode import unidecode
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

from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislatorScraperUtils
from pathlib import Path
import boto3

scraper_utils = USStateLegislatorScraperUtils(
    'KY', 'us_ky_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://legislature.ky.gov')


def request_find(base_url, t, att, filter_all=False):
    url_request = requests.get(base_url, verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    if filter_all:
        return url_soup.find_all(t, att)
    return url_soup.find(t, att)


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


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    senate_members_url = 'https://legislature.ky.gov/Legislators/senate'
    house_members_url = 'https://legislature.ky.gov/Legislators/house-of-representatives'

    page = scraper_utils.request(senate_members_url)
    soup = BeautifulSoup(page.content, 'lxml')

    # Get url of current year assymbly members
    try:
        content = soup.find('div', {'id': 'cbqwpctl00_ctl00_m_g_4af53f99_1f77_4ed2_a980_056e3cfc19c5'})

        links = content.find_all("a")
        for link in links:
            link = "https://legislature.ky.gov" + link['href']
            urls.append(link)
        # print(link)
    except:
        pass
    page2 = scraper_utils.request(house_members_url)
    soup2 = BeautifulSoup(page2.content, 'lxml')

    try:
        content = soup2.find('div', {'id': 'cbqwpctl00_ctl00_m_g_a017c22c_6ccc_4063_a136_39f87b11c5f7'})
        links2 = content.find_all("a")
        for link in links2:
            link = "https://legislature.ky.gov" + link['href']
            urls.append(link)
        # print(link)
    except:
        pass
    # return [['Legislators/Pages/Legislator-Profile.aspx?DistrictNumber=68', "House"]]
    return urls


def get_wiki_url(row):
    wikipage_reps = "https://ballotpedia.org/Kentucky_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Kentucky_State_Senate"

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
            return link


def scrape(url):
    print(url)
    try:
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
        url_request = requests.get(url, verify=False)
        url_soup = BeautifulSoup(url_request.content, 'lxml')
        row = scraper_utils.initialize_row()

        row.source_url = url
        # print(url_soup)
        try:
            committee_list = url_soup.find('div', {'id': 'legcommittees'}).find_all('li')
            committees = []
            for commitee in committee_list:
                # pprint(commitee.get_text(strip=True).split('\t', 1))
                somelist = commitee.get_text(strip=True).split('\t', 1)
                committee = somelist[0].replace('\n', '').replace('\r', '').replace('\t', '')
                role = somelist[1].replace('\n', '').replace('\r', '').replace('\t', '')
                committees.append({'committee': committee,
                                   'role': role})

            row.committees = committees
        except:
            pass
        # pprint(f'committess: {committees}')

        """
        https://stackoverflow.com/questions/57392407/how-to-split-html-text-with-br-tags
        """
        areas_served = []
        phone_numbers = []
        email = ""
        p_list = url_soup.find_all('p')
        for index, p in enumerate(p_list):
            target = p.text
            if "Vacant Seat" in target:
                row.areas_served = None
                row.email = ""
                break
            if target == "Home City":
                areas_served = [p_list[index + 1].text]
                row.areas_served = areas_served
            elif target == "Phone Number(s)":
                phone_number_list = p_list[index + 1].get_text(separator='|', strip=True).split('|')
                for number in phone_number_list:
                    phone_numbers.append({'number': number.split(": ")[1],
                                          'type': number.split(": ")[0]})
                row.phone_number = phone_numbers
            elif target == "Email":
                email = p_list[index + 1].text
                row.email = email

        # pprint(f'areas_served: {areas_served}')
        # pprint(f'email: {email}')
        # pprint(f'phone_numbers: {phone_numbers}')

        # pprint(areas_served)
        try:
            district = url_soup.find('div', {'class': 'circle'}).text
            row.district = district
        except:
            pass

        # pprint(f'district: {district}')

        NAME_KEYS = ['name_first', 'name_last', 'name_middle']

        full_name_unfiltered = url_soup.find('div', {'class': 'row profile-top'}).find('h2').text
        role = full_name_unfiltered.split(" ")[0]
        full_name_unfiltered = full_name_unfiltered.split(" ")[1:-1]
        name_dict = {'name_full': '',
                     'name_first': '',
                     'name_middle': '',
                     'name_last': '',
                     'role': ''}

        temp = []
        for name in full_name_unfiltered:
            if name != "":
                temp.append(name)

        full_name = ""
        for index, name in enumerate(temp):
            if len(temp) == 3 and index == 1:
                name_dict[NAME_KEYS[index + 1]] = name
                full_name = full_name + name + " "
            else:
                if len(temp) == 3 and index == 2:
                    name_dict[NAME_KEYS[index - 1]] = name
                else:
                    name_dict[NAME_KEYS[index]] = name
                full_name = full_name + name + " "

        name_dict["name_full"] = full_name[:-1]
        name_dict['role'] = role

        print(f'name info: {name_dict}')
        row.name_first = name_dict["name_first"]
        row.name_middle = name_dict["name_middle"]
        row.name_last = name_dict["name_last"]
        row.name_full = name_dict["name_full"]
        row.role = name_dict["role"]

        titles = ["Mailing Address", 'Legislative Address', 'Capitol Address']
        addresses = url_soup.find_all('address')
        address_list = []
        try:
            address_titles = url_soup.find('div', {'class': 'relativeContent col-sm-4 col-xs-12'}).find_all('p', {
                'class': 'title'})[0:]
            index = 0
            for title in address_titles:
                if title.text in titles:
                    address_list.append({'address': addresses[index].text,
                                         'location': title.text})
                    index += 1
        except:
            pass
        # pprint(f'address: {address_list}')

        row.addresses = address_list
        try:
            row.wiki_url = get_wiki_url(row)
        except:
            pass
        try:
            gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
            if not gender:
                gender = 'O'
            row.gender = gender
        except:
            pass
        print(row)
        return row
    except:
        pass


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()
    print(urls)
    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    #try:
    data = [scrape(url) for url in urls]
    # with Pool() as pool:
    #     data = pool.map(scrape, urls)
    #     # pprint(data)
    #     # Once we collect the data, we'll write it to the database.
    leg_df = pd.DataFrame(data)
    leg_df.drop(leg_df.index[leg_df['name_full'] == ''], inplace=True)
        # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Kentucky_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Kentucky_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                          on=["name_last", 'wiki_url'])

    print('Scraping complete')

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
