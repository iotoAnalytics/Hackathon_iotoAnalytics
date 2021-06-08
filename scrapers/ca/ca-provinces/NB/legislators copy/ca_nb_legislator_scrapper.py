'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
from operator import add
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils, ScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
from nameparser import HumanName
import re
import boto3
import time


province_territory = 'NB'
prov_terr_abbreviation = 'NB'
database_table_name = 'ca_nb_legislators'

scraper_utils = CAProvTerrLegislatorScraperUtils(province_territory,database_table_name)

base_url = 'https://www2.gnb.ca/'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def get_between_parentheses(text):
    return text[text.find("(")+1:text.find(")")]



def get_urls():


    path = 'content/gnb/en/contacts/MLAReport.html'
    scraper_url = base_url + path

    page = scraper_utils.request(scraper_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    mla_list = soup.find('div', {'id': 'search_column_full'})
    
    mla_list = mla_list.find_all('tr')

    
    
    info_list = []
    row_type = 0
    for mla in mla_list:
        if row_type == 0 :
            info = mla.find_all('td')[1].find('a')
            link = info.get('href')
            print(link)
            row_type = 1

            info_list.append(link)

        elif row_type == 1:
            row_type = 2
        else:
            row_type = 0
        
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    


   
    #path = '/legis/bios/60/index-e.asp'
    #scrape_url = base_url + path
    #page = scraper_utils.request(scrape_url)
    #soup = BeautifulSoup(page.content, 'html.parser')
    #mla_list = soup.find('table', {'id': 'example'})
    #mla_list = mla_list.find('tbody')
    #mla_list = mla_list.find_all('tr')
    #for mla in mla_list:
     #   info_list = mla.find_all('td')

     #   print(info_list[1])
     #   print(info_list[2])
     #   print(info_list[3])
     #   print('\n')
     #   mla_info = {
     #       'link'
    #        'email'
      #      'party'
      #      'riding'
            
      #  }


    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return info_list
def get_contact_info(soup):
    contact_soup = soup.find('div',{'class':'renderer_container'})

    district_info = contact_soup.find('div',{'class':'gnbpage'}).getText().split(',')
    district_name = district_info[0]
    district_number = int(re.search(r'\d+', district_info[1]).group())

    contact_info = contact_soup.findAll('div',{'class':'column'})
    address = contact_info[1].find('p')
    address = address.getText(strip=True, separator=" ").replace("Constituency Office: ",'')
    
    contact_info = contact_info[0].find('p').getText().split(' ')
    
    phone_number = contact_info[2] + contact_info[3]

    phone = {
        'phone':'phone',
        'number':phone_number
    }
    phone_list = [].append(phone)
    email = contact_info[-2]
    addressList = []
    address = {
        'location':'Constituency Office',
        'address': address}
    addressList.append(address)
    print(email)
    print(address)
    print(phone_number)

    print(district_name)
    print(district_number)

    info = {
        'district_name': district_name,
        'district_number' : district_number,
        'phone_number' : phone_list,
        'address' : addressList,
        'email' : email
    }
    
    return info

def process_name(name_text):
    try:
        party = name_text[name_text.find("(")+1:name_text.find(")")]
    except:
        print('no party found')

    if "HON." in name_text :
        name_text = name_text.replace('HON.','')
    
    processed_name = HumanName(name_text)
    

    return dict(full = name_text, first = processed_name.first, last = processed_name.last, middle = processed_name.middle,suffix = processed_name.suffix, party = party)


def scrape(url):
    row = scraper_utils.initialize_row()
    try:
        
        path = 'content/gnb/en/contacts/'
        scrape_url = base_url + path + url
        page = scraper_utils.request(scrape_url)
        soup = BeautifulSoup(page.content, 'html.parser')

        contact_info = get_contact_info(soup)

        name = soup.find('div',{'id':'mainContent'})
        name = name.find('h1').getText()

        processed_name = process_name(name)
        print(processed_name['first'])
        print(processed_name['last'])
        


        row.source_url = scrape_url
        row.name_full = processed_name['full']
        row.name_first = processed_name['first']
        row.name_last = processed_name['last']
        row.name_middle = processed_name['middle']
        row.name_suffix = processed_name['suffix']

        row.source_id = contact_info['disctrict_number']
        row.riding = contact_info['district_name']
        row.email = contact_info['email']
        row.addresses = contact_info['address']
        row.phone_numbers = contact_info['phone_number']
        row.regions = scraper_utils.get_region(prov_terr_abbreviation)


        # ... Collect data from page

    # Delay so we don't overburden web servers
        scraper_utils.crawl_delay(crawl_delay)
    except:
        ('failure')

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Scrape data from collected URLs serially, which is slower:
    # data = [scrape(url) for url in urls]
    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
