'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
from pprint import pprint
from nameparser import HumanName
import re
import boto3
import time
import datetime


state_abbreviation = 'NV'
database_table_name = 'us_nv_legislators'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

# !! 1.
original_url  = 'https://www.leg.state.nv.us/'

base_url = original_url +'/App/Legislator/A/Assembly/Current'
base_senate_url = original_url + '/App/Legislator/A/Senate/Current'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

#def get_name(content) :
  #  name = content.find_all('td')[1].find('a')
   # name.find('br').replace_with('\n')
   # name = re.sub("\n.*",'',name.text)
    #print(name)
   # return name


#gets the district
def get_district(content) :
    district = content.find_all('td')[3].find('a').text
    district = re.findall("\d+", district)[0]
    print(district)
    return district

#gets the party
def get_party(content) :
    party = content.find_all('td')[2].find('a').text
    if party == "Democratic" :
        party = "Democrat"
    print(party)
    return party

#gets the county
def get_county(content) :
    county = content.find_all('td')[4].find('a')
    print(county)
    return county.text

#gets the link
def get_link(content) :
    link = content.find_all('td')[0].find('a')
    link = link.get('href')
    print(link)
    return link

#splits the name 
def get_split_name(full_name) :
    name_parts = full_name.split(' ',1)
    position = name_parts[0]
    fn = name_parts[1]
    name = HumanName(fn)
    if 'Assembly' in position :
        position = 'Representative'
    

    name = {
        'full_name': fn,
        'first_name': name.first,
        'middle_name': name.middle,
        'last_name': name.last,
        'suffix': name.suffix,
        'position': position
    }

    return name

#gets all the urls to scrape and returns a dict with urls and extra information
def get_urls(url) : 
    sentate = 0
    if ('Senate' in url) :
        sentate = 1

    term_id = None
    page = scraper_utils.request(url)
    page = BeautifulSoup(page.content, 'lxml')
    url_table = page.find('tbody')
    url_links = url_table.find_all('tr',{'class':'thisRow'})
    count = 0

    legislators = []
    district = None
    party = None
    county = None
    link = None

    # !! 3.
    for item in url_links:
        if(count % 2 == 0 ) :
            
            district = get_district(item)
            party = get_party(item)
            county = get_county(item)
            link = get_link(item)
            
        else:
            # !! 4.
            phoneNumbers = []
            print(county)
            content = item.find_all('td')
            address_list = []
            address = {
                'location': 'office',
                'address' : " ".join(content[0].text.split())
            }
            address_list.append(address)
            content = content[1].find_all('span',{'class':'field'})
            if (sentate == 0):
                term_id = int(content[0].text) - 2
            else :
                term_id = int(content[0].text) - 6

            email = content[1].text
            office_no = {
                'office' : 'Leg Bldg Phone:',
                'number' : content[3].text.replace('(','').replace(')','')
            }
            phoneNumbers.append(office_no)
            try:
                work_no = {
                'office' : 'Work Phone:',
                'number' : content[4].text.replace('(','').replace(')','')
                }
                phoneNumbers.append(work_no)

            # !! 2.
            except Exception:
                print("no phone number")

            print(address)
            print(email)

            
            legislator = {
                'term_id': term_id,
                'link':(original_url + link),
                'district':district,
                'party':party,
                'county':county,
                'phoneNo':phoneNumbers,
                'address':address_list,
                'email':email
            }
            legislators.append(legislator)
        
        count += 1

   

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return legislators

#gets the active years from 
def get_years(content):
    active_years = []
    year_ranges = re.findall("\d{4}-Present", content.text)

    for range in year_ranges:
        active_years = active_years + get_range(range)
    
    year_ranges = re.findall("\d{4}-\d{4}", content.text)

    for range in year_ranges:
        active_years = active_years + get_range(range)

    if not active_years :
        year = re.findall("\d{4}", content.text)
        active_years.append(int(year[0]))

    return active_years


#helper function that takes a range of years and returns a list of years
def get_range(years):
    year_range = []

    numbers = years.split('-')
    if("Present" in years) :
        year_range = range(int(numbers[0]), datetime.datetime.now().year)
    else :
        year_range = range(int(numbers[0]),int(numbers[1]))

    return list(year_range)

# gets the committes the legislator is affiliated with
def get_committees(content):
    committees = []
    com_soup = content.find_all('li')
    

    for item in com_soup :
        text = item.text.rsplit(' ', 1)
        committee = {
            'role': text[1].replace('(','').replace(')',''),
            'committee': text[0]
        }
        print (committee)
        committees.append(committee)
    return committees

#gets the education from the legislator
def get_education(content):
    print (content)
    level = ''
    field = ''
    school = ''

    education_parts = content.split(',')
    if("High School" in education_parts[0]) :
        school = education_parts[0].strip()
    elif len(education_parts) == 1 :    
        school = education_parts[0]
    elif len(education_parts) == 3 :
        level = education_parts[1].strip()
        field = education_parts[2].strip()
        school = education_parts[0].strip()
    elif len(education_parts) == 4:
        level = education_parts[2].strip()
        field = education_parts[3].strip()
        school = education_parts[0].strip()


    education = {
        'level':level,
        'field': field,
        'school':school
    }
    if(education['school'] == ''):
        return None

    return education

#returns the occupation of the legislator
def get_occupation(content) :
    occupations = []
    content = content.find_all('div')[1].text.strip()
    
    occupation_parts = content.split('\n')
    occupation_parts = occupation_parts[0].split(':')

    print(occupation_parts)
    if (occupation_parts[0] == 'Occupation'):
        for job in occupation_parts[1].split(','):
            print(job)
            occupations.append(job)
    return occupations

#gets the personal informations of the legislator
def get_personal(content) :
    active_years = []
    education = []
    occupation = []
    
    data_type = -1
    for item in content.find_all(recursive=False):
        print (item.name)
        if (item.name == "div") :
            if(item.text.strip() == "Legislative Service") :
                data_type = 0
            elif(item.text.strip() == "Education"):
                data_type = 1
            else :
                data_type = -1
        else:
            if(data_type == 0) :
                active_years = get_years(item)
            elif (data_type == 1) :
                education.append(get_education(item.text))

    occupation = occupation + get_occupation(content.find('div',{'id':'personalInfoDiv'}))
    active_years = list(dict.fromkeys(active_years))
    education = list(filter(None, education))
    personal = {
        'years_active': active_years,
        'education': education,
        'occupation': occupation
    }
    return personal

#returns a list of areas served by the legislator
def get_area_served(content) :
    areas_served = []
    area_list = content.find_all('h4')
    area_list = area_list[1].text
    area_list = area_list.split(':')[1].split(',')
    for area in area_list :
        areas_served.append(area.replace('(Part)','').strip())
    return areas_served

#scrapes the urls and inserts data into the DB
def scrape(legislator):

    committees = []
 
    print (legislator['link'])
    
    # Send request to website
    page = scraper_utils.request(legislator['link'])
    row = scraper_utils.initialize_row()

    

    page = BeautifulSoup(page.content, 'lxml')
    soup_personal = page.find('div',{'id':'personalCareerTab'})

    title = page.find('h3')
    print(title)
    name = get_split_name(title.text)
    print(title)

    soup_commitees = page.find('div',{'id':'committeesTab'})
    soup_district = page.find('div',{'id':'districtTab'})
    area_served = get_area_served(soup_district)
    committees = get_committees(soup_commitees)
    personal = get_personal(soup_personal)
    row.most_recent_term_id = legislator['term_id']
    row.name_full = name['full_name']
    row.source_id = legislator['link'].rsplit("/", 1)[1]
    row.name_first = name['first_name']
    row.name_middle = name['middle_name']
    row.name_last = name['last_name']
    row.name_suffix = name['suffix']
    row.role = name['position']
    row.source_url = legislator['link']
    row.party = legislator['party']
    row.years_active = personal['years_active']
    row.committees = committees
    row.phone_numbers = legislator['phoneNo']
    row.addresses = legislator['address']
    row.email = legislator['email']
    row.areas_served = area_served
    row.district = legislator['district']
    row.party_id = scraper_utils.get_party_id(legislator['party'])
    row.occupation = personal['occupation']
    row.education = personal['education']

    

    # Delay so we don't overburden web servers
    
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls(base_url)
    urls = urls + get_urls(base_senate_url)

    # Scrape data from collected URLs serially, which is slower:
    # data = [scrape(url) for url in urls]
    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
