"""
Author: Avery Quan
Date Created: Jan 13, 2021
Function: Scrape legislator data for the state of Alabama
Issues:
    - names with only Initials (J.B.) has J as first name and B as middle name
    - birthdays on wikipedia that don't have class='bday' only get the years scraped
"""
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))


import requests
from bs4 import BeautifulSoup
import request_url
import pandas as pd
# from database import Database
# from database import CursorFromConnectionFromPool
from psycopg2 import sql
import json
import datetime
from nameparser import HumanName
from nameparser.config import CONSTANTS
import html
import re
from legislator_scraper_utils import LegislatorScraperUtils

'''temp imports'''
from pprint import pprint



header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
senators_url = 'http://www.legislature.state.al.us/aliswww/ISD/Senate/ALSenators.aspx'
senator_pic_url = 'http://www.legislature.state.al.us/aliswww/ISD/ALSenator.aspx' #first half for personal page url
house_of_rep_url = 'http://www.legislature.state.al.us/aliswww/ISD/House/ALRepresentatives.aspx'
house_of_rep_pic_url = 'http://www.legislature.state.al.us/aliswww/ISD/ALRepresentative.aspx' #first half for personal page url

wikipedia_house_url = 'https://en.wikipedia.org/wiki/Alabama_House_of_Representatives'
wikipedia_senate_url = 'https://en.wikipedia.org/wiki/Alabama_Senate'


def strip_name(pic_url, is_senate):
    if is_senate:
        pic_url = pic_url.replace('../../Senate/Senator%20Pictures/', '')
    else:
        pic_url = pic_url.replace('../../House/Representative%20Pictures/', '')
    name = pic_url.split('_', 1)[0]
    return name

'''
replace nan values with ''
'''
def nan(string):
    string = str(string).replace('nan, ', '')
    string = string.replace('nan', '')
    return string


def get_legislator_links(base_url, is_senate, pic_url):

    member_request = request_url.UrlRequest.make_request(base_url, header)
    member_soup = BeautifulSoup(member_request.content, 'lxml')
    members = member_soup.find('table').find_all('input')

    links = {}

    """
    SENATOR URL COMPONENTS EXAMPLE
    url:  http://www.legislature.state.al.us/aliswww/ISD/ALSenator.aspx?NAME=Barfoot&OID_SPONSOR=100468&OID_PERSON=8486&SESSNAME=Regular%20Session%202021
    name: src
    oid_sponsor: longdesc
    oid_person: alt
    session: SESSNAME=Regular%20Session%20 + current_year
    """

    for member in members:
        member_url = pic_url
        name = 'NAME=' + strip_name(member['src'], is_senate)
        oid_sponsor = 'OID_SPONSOR=' + member['longdesc']
        oid_person = 'OID_PERSON=' + member['alt']
        session = 'SESSNAME=Regular%20Session%20' + str(datetime.datetime.now().year)

        member_url = member_url + '?' + name  + '&' + oid_sponsor  + '&' + oid_person + '&' + session
        links[member_url] = {'session': session, 'oid_sponsor': oid_sponsor}

    return links



def scrape_legislator(links):
    CONSTANTS.titles.remove(*CONSTANTS.titles)
    party = {'(D)':'Democrat', '(R)':'Republican'}

    legislators = {}

    for link, value in links.items():

        # base_url = 'http://www.legislature.state.al.us/aliswww/ISD/ALRepresentative.aspx?NAME=Alexander&OID_SPONSOR=100537&OID_PERSON=7710&SESSNAME=Regular%20Session%202021'
        base_url = link
        member_request = request_url.UrlRequest.make_request(base_url, header)
        member_soup = BeautifulSoup(member_request.content, 'lxml')
        members = member_soup.find_all('table')

        complete_name = member_soup.find('span', id='ContentPlaceHolder1_lblMember').string

        # unescape turns &QUOT; into ""
        name = html.unescape(complete_name.string)
        name = html.unescape(complete_name.string)
        name = name.replace('SENATOR', '')
        name = name.replace('REPRESENTATIVE', '')
        name = HumanName(name)
        if name.last == '':
            continue

        name.capitalize()

        legislature_table = pd.read_html(str(members[0]))[0]
        district_table = pd.read_html(str(members[1]))[0]
        committees_table = pd.read_html(str(members[-1]))[0]



        fields = scraper_utils.initialize_row()

        fields.state_member_id = value['oid_sponsor'].replace('OID_SPONSOR=', '')
        fields.most_recent_term_id = value['session'].replace('%20', '').replace(' ', '').replace('SESSNAME=', '')
        fields.date_collected = datetime.datetime.today().strftime('%d-%m-%Y') 
        fields.state_url = base_url
        fields.name_full = name.full_name
        fields.name_last = name.last
        fields.name_first = name.first
        fields.name_middle = name.middle
        fields.name_suffix = name.suffix
        fields.party = party[district_table[1][0]]
        fields.party_id = scraper_utils.get_party_id(fields.party)
        fields.role = complete_name.split()[0].title()
        fields.district = district_table[1][1].split()[2]

        try:
            committees_table.index  = committees_table['Committees'].tolist()
            temp = []
            for index, row in committees_table.iterrows():
                temp.append(
                    {
                    'committee': row['Committees'],
                    'role': row['Position']
                })

            fields.committees = temp


            # fields.committees'] = temp

        except KeyError:
            print('key error')
            pass
        
        fields.areas_served = district_table[1][2].split(',')

        temp = []
        if nan(district_table[1][3]) != '':
            temp.append({'number': nan(district_table[1][3]), 'office': 'District Office'})
        if legislature_table[1][3] != '':
            temp.append({'number': legislature_table[1][3], 'office': 'Capitol Office'})

        fields.phone_number = temp

        temp = []
        # This just puts all the address components together, nan turns nan values to ''
        temp.append({'location': 'Capitol Office', 'address': nan(str(legislature_table[1][6]).replace('Suite', '')) + ' ' + legislature_table[1][5] + ', ' + 
                                                            legislature_table[1][7] + ', AL ' + legislature_table[1][9]})

        temp.append({'location': 'District Office', 'address': nan(str(district_table[1][5]) + ', ') + nan(str(district_table[1][6]) + ', ') + nan(district_table[1][7]) + 
                                                                ', AL, ' + nan(district_table[1][9])})  

        fields.addresses = temp

        email = legislature_table[1][10]
        fields.email = nan(email if email == email else district_table[1][10]) # email == email checks if email = nan or not

        legislators[fields.district] = fields
    return legislators

def get_wiki_links(link):
    wikipedia_link = 'https://en.wikipedia.org'

    member_request = request_url.UrlRequest.make_request(link, header)
    member_soup = BeautifulSoup(member_request.content, 'lxml')
    members = member_soup.find('table', class_='wikitable sortable')    
    members = members.find_all('tr')[1:]  

    links = {}

    for member in members:
        district = member.find_all('td')[0].find('a').text
        page_url_end = member.find_all('td')[1].find('a', class_=False)
        if page_url_end is None:
            continue

        links[district] = (wikipedia_link + page_url_end['href'])

    return links

def scrape_wiki(links):
    missing_fields = {}
    for district, link in links.items():
        member_request = request_url.UrlRequest.make_request(link, header)
        member_soup = BeautifulSoup(member_request.content, 'lxml')
        table = member_soup.find('table', class_='infobox vcard')   
        if table is None:
            continue

        panda = pd.read_html(str(table))[0]
        panda = panda.dropna()
        panda.columns = {'Key', 'Value'}
        key = panda.columns[0]

        missing_fields[district] = {
            'years_active': [],
            'birthday': None,
            'occupation': [],
            'education': {}
        }

        try:
            # gets cell for the year legislator assumed office from a dataframe, then extracts the year from the string in that cell
            years_active = re.sub('\[.*\]', '', panda[panda[key].str.contains('Assumed office',regex=False)]['Key'].values[0])[-4:]
            # creates a list for the range of years active
            years_active = list(range(int(years_active), datetime.date.today().year + 1))
            missing_fields[district]['years_active'] = years_active
        except Exception as e:
            print(str(e))

        try:
            years = panda[panda[key].str.contains('In office',regex=False)]['Key'].values

            for year in years:
                year_range = re.findall('\d\d\d\d', year)
                # creates a list for the range of years active
                years_active = list(range(int(year_range[0]),int(year_range[1]) + 1))
                missing_fields[district]['years_active'] = list(set(years_active + missing_fields[district]['years_active']))
                
        except Exception as e:
            print(str(e))
            pass

        try: 
            rep_birth = table.find('span', {"class": "bday"}).text
            birthday = datetime.datetime.strptime(rep_birth, "%Y-%m-%d").date()
            missing_fields[district]['birthday'] = birthday
        except Exception as e:
            try:
                # sometimes the bday tag isnt there on some pages so we use a lambda, slower
                rep_birth = table.find(lambda tag:tag.name=="tr" and "Born" in tag.text).text
                year_birth = re.findall('\d\d\d\d', rep_birth)[0]
                birthday = datetime.datetime.strptime(year_birth, "%Y").date()
                missing_fields[district]['birthday'] = birthday
            except Exception as e:
                pass
            
        
        try:
            occupation = table.find(lambda tag:tag.name=="tr" and "Profession" in tag.text).text
            occupation = occupation.replace('occupation', '')
            occupation = occupation.replace('Profession', '')
            missing_fields[district]['occupation'] = occupation.split(',')
        except Exception as e:
            pass

        """
        Notes:
            Majority of code in lower try block is Anikka's, with some improvements to include missing education levels
        """
        try:
            lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD", "DDS", "MS", "BS", "MBA", "MS", "MD"]
            reps = member_soup.find("div", {"class": "mw-parser-output"})
            # repsAlmaMater = reps.find("th", {"scope:" "value"})
            left_column_tags = reps.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if lefttag.text == "Alma mater" or lefttag.text == "Education":
                    index = left_column_tags.index(lefttag) + 1
                    next = left_column_tags[index]
                    #find text lines with either University, College, School 
                    alines = next.findAll(lambda tag: "University" in tag.text or "College" in tag.text or "School" in tag.text or ')' in tag.text or re.sub('[^a-zA-Z]+', "", tag.text) in lvls)
                    for aline in alines:
                        level = ''
                        school = ''
                        if "University" in aline.text or "College" in aline.text or "School" in aline.text:
                            school = aline.text
                            # this is most likely a school
                            level = ""
                            try:
                                lineIndex = alines.index(aline) + 1
                                nextLine = alines[lineIndex].text
                                if '(' in nextLine or re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
                                    level = nextLine

                            except:
                                pass
                            edinfo = {'level': level, 'field': "", 'school': school}
                            missing_fields[district]['education'] = edinfo
        except Exception as e:
            print(str(e))
            pass
    # pprint(missing_fields)

    return missing_fields

def merge_wiki(wiki, legislators):
    for district, value in wiki.items():
        try:
            legislators[district] = {**legislators[district], **value}

        except Exception as e:
            print('Wikipedia has a legislator from district ' + district + ' that the Alabama legislator website does not' )
    
    legislators = dict(sorted(legislators.items()))
    return legislators
    


# def init_database():
#     db_user = 'postgres'
#     db_pass = 'dionysos'
#     db_host = 'openparl.cia2zobysfwo.us-west-2.rds.amazonaws.com'
#     db_port = '5432'
#     db_name = 'openparl'   

#     Database.initialise(database=db_name, host=db_host, user=db_user, password=db_pass)
    
def dict_to_list(dictionary):
    my_list = []

    for key, value in dictionary.items():
        my_list.append(value)

    return my_list


# init_database()
scraper_utils = LegislatorScraperUtils('AL', 'us_al_legislators', 'United States of America')

#house scraper
house_wiki_links = get_wiki_links(wikipedia_house_url)
house_wiki = scrape_wiki(house_wiki_links)
house_links = get_legislator_links(house_of_rep_url, False, house_of_rep_pic_url)
house_dict = scrape_legislator(house_links)
house = merge_wiki(house_wiki, house_dict)
house = dict_to_list(house)

#senate scraper
senate_wiki_links = get_wiki_links(wikipedia_senate_url)
senate_wiki = scrape_wiki(senate_wiki_links)
senate_links = get_legislator_links(senators_url, True, senator_pic_url)
senate_dict = scrape_legislator(senate_links)
senate = merge_wiki(senate_wiki, senate_dict)
senate = dict_to_list(senate)

senate_house_data_lst = house + senate


# scraper_utils.insert_legislator_data_into_db(senate_house_data_lst)

for d in senate_house_data_lst[:10]:
    print(d)


