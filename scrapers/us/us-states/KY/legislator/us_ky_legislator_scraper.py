import re
from datetime import date, datetime
from nameparser import HumanName
from pprint import pprint
import configparser
import sys, os
from pathlib import Path

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
crawl_delay = scraper_utils.get_crawl_delay('https://www.legis.nd.gov')



def request_find(base_url, t, att, filter_all=False):
    url_request = requests.get(base_url, verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    if filter_all:
        return url_soup.find_all(t, att)
    return url_soup.find(t, att)

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    senate_members_url = 'https://legislature.ky.gov/Legislators/senate'
    house_members_url = 'https://legislature.ky.gov/Legislators/house-of-representatives'

    # Get url of current year assymbly members
    content = request_find(senate_members_url, 'div', {'id': 'cbqwpctl00_ctl00_m_g_4af53f99_1f77_4ed2_a980_056e3cfc19c5'})
    for link in content.find_all('a'):
        urls.append(link['href'])

    content = request_find(house_members_url, 'div', {'id': 'cbqwpctl00_ctl00_m_g_4af53f99_1f77_4ed2_a980_056e3cfc19c5'})
    for link in content.find_all('a'):
        urls.append(link['href'])
    # return [['Legislators/Pages/Legislator-Profile.aspx?DistrictNumber=68', "House"]]
    return urls

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
    base_url = 'https://legislature.ky.gov/'
    url_request = requests.get(base_url + url, verify=False)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    row = scraper_utils.initialize_row()

    row.source_url = base_url + url


    committee_list = url_soup.find('div', {'id':'legcommittees'}).find_all('li')
    committees = []
    for commitee in committee_list:
        # pprint(commitee.get_text(strip=True).split('\t', 1))
        somelist = commitee.get_text(strip=True).split('\t', 1)
        committee = somelist[0].replace('\n', '').replace('\r', '').replace('\t', '')
        role = somelist[1].replace('\n', '').replace('\r', '').replace('\t', '')
        committees.append({'committee': committee,
                           'role': role})

    # pprint(f'committess: {committees}')

    row.committees = committees

    """
    https://stackoverflow.com/questions/57392407/how-to-split-html-text-with-br-tags
    """
    areas_served = None
    phone_numbers = []
    email = None
    p_list = url_soup.find_all('p')
    for index, p in enumerate(p_list):
        target = p.text
        if target == "Home City":
            areas_served = [p_list[index+1].text]
        elif target == "Phone Number(s)":
            phone_number_list = p_list[index+1].get_text(separator='|', strip=True).split('|')
            for number in phone_number_list:
                phone_numbers.append({'number': number.split(": ")[1],
                                      'type': number.split(": ")[0]})
        elif target == "Email":
            email = p_list[index+1].text

    # pprint(f'areas_served: {areas_served}')
    # pprint(f'email: {email}')
    # pprint(f'phone_numbers: {phone_numbers}')
    row.areas_served = areas_served
    row.phone_number = phone_numbers
    row.email = email
    # pprint(areas_served)

    district = url_soup.find('div', {'class': 'circle'}).text
    row.district = district

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
            name_dict[NAME_KEYS[index+1]] = name
            full_name = full_name + name + " "
        else:
            if len(temp) == 3 and index == 2:
                name_dict[NAME_KEYS[index -1]] = name
            else:
                name_dict[NAME_KEYS[index]] = name    
            full_name = full_name + name + " "

    name_dict["name_full"] = full_name[:-1]
    name_dict['role'] = role

    # pprint(f'name info: {name_dict}')
    row.name_first = name_dict["name_first"]
    row.name_middle = name_dict["name_middle"]
    row.name_last = name_dict["name_last"]
    row.name_full = name_dict["name_full"]
    row.role = name_dict["role"]

    titles = ["Mailing Address", 'Legislative Address', 'Capitol Address']
    addresses = url_soup.find_all('address')
    address_list = []
    address_titles = url_soup.find('div', {'class': 'relativeContent col-sm-4 col-xs-12'}).find_all('p', {'class': 'title'})[0:]
    index = 0
    for title in address_titles:
        if title.text in titles:
            address_list.append({'address': addresses[index].text,
                                 'location':title.text}) 
            index+=1
    
    # pprint(f'address: {address_list}')

    row.addresses = address_list


    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()
    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    try:
        # data = [scrape(url) for url in urls]
        with Pool() as pool:
            data = pool.map(scrape, urls)
        # pprint(data)
        # Once we collect the data, we'll write it to the database.
        scraper_utils.write_data(data)

    except Exception as e:
        sys.exit(f'error: {e}\n')
    print('Complete!')