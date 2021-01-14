import requests
from bs4 import BeautifulSoup
import request_url
import pandas as pd
import re
import pprint
import datetime

pp = pprint.PrettyPrinter(indent=4)

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
base_url = 'https://www.legis.nd.gov/assembly'

POLITICAL_PARTIES = ['Republican', 'Democrat', "Libertarian", 'Green', 'Consitituion']
GOVERLYTIC_ID = 0
URL_TEMPLATE = 'us/nd/'
COUNTRY_ID = 1
COUNTRY = 'United States'
STATE_ID = 99
STATE = 'ND'
PARTY_ID = 1 
BIRTHDAY_NONE = None
MILITARY_EXPERIENCE_NONE = ''
EDUCATION_NONE = []
STATE_MEMBER_ID = ''
SENIORITY = None
def pprint(str):
    return pp.pprint(str)

def request_find(base_url, t, att, filter_all=False):
    url_request = request_url.UrlRequest.make_request(base_url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    if filter_all:
        return url_soup.find_all(t, att)
    return url_soup.find(t, att)

# Either filters first then returns first "a" tag with href or returns first a tag href found
# Assumes a tag has href


def retrieve_href(base_url, t, att, filter=False, filter_t=None, reg=None):
    content = request_find(base_url, t, att)
    if filter:
        filtered_content = content.find(filter_t, re.compile(reg))
        return filtered_content.a['href']
    return content.a['href']


def retrieve_legislator_urls(base_url):
    assembly_info_url = retrieve_href(
        base_url, 'div', {'class': 'view-content'}, True, 'li', '^first*')
    assembly_members_url = retrieve_href(assembly_info_url, 'div', {
                                         'class': 'panel-pane pane-custom pane-1'})

    member_urls = []
    content = request_find(assembly_members_url, 'div',
                           {'class': 'name'}, True)
    for member in content:
        member_urls.append(member.a)

    return member_urls


# Each url containts information on one member of the assembly
legislators_list = retrieve_legislator_urls(base_url)

VERBOSE = False
def retrieve_information(lst_href):
    data = []
    global GOVERLYTIC_ID
    for state_url in lst_href:
        state_url = state_url['href']
        if state_url == None:
            print("Error state url: " + str(state_url) + '\n')
            continue
        legislator_info = []
        url_request = request_url.UrlRequest.make_request(
            state_url, header)
        url_soup = BeautifulSoup(url_request.content, 'lxml')

        # name_content = url_soup.find('h1', {'class': 'title', 'id': 'page-title'}).text.split(re.compile('\s|,|[^,\s]+'))
        date_collected = datetime.datetime.now()
        current_year = date_collected.year
        current_term = str(current_year) + '-' + str(current_year + 1)
        name_content = re.split(
            r'; |, |\*|\n', url_soup.find('h1', {'class': 'title', 'id': 'page-title'}).text)
        suffix = ''
        middle_name = ''
        if len(name_content) > 1:
            suffix = name_content[1] + ' '
        name_content = name_content[0].split(" ")
        for i in range(len(name_content)):
            if i == 0:
                role = name_content[i]
            elif i == 1:
                first_name = name_content[i]
            elif i == 2 and len(name_content) > 3:
                middle_name = name_content[i] + ' '
            else:
                last_name = name_content[i]
        full_name = suffix  + first_name + ' ' + middle_name + last_name

        content = url_soup.find('div', {'id': "block-system-main",
                                        'class': 'block block-system first last odd'})
        biography_items = content.find('div', {'class': 'panel-pane pane-node-body biography'}).find_all('li')
        occupation = {''}
        years_active = []
        if len(biography_items) > 0:
            occupation = biography_items[0].text.split(';')
            years_active = re.findall(r'(\d{4}-(?:\d{4}|\d{2})|\d{4})',biography_items[len(biography_items) - 1].text)

        district_number = content.find('div', 'pane-content').find('a').text.split(' ')[1]
        string_contaning_party = content.find('div', 'pane-content').text
        legislator_party = ''
        for party in POLITICAL_PARTIES:
            if party in string_contaning_party:
                legislator_party = party
                break
        
        committees_lst = []
        committees = content.find_all('div', 'cmte-item')
        for committee in committees:
            committee_dict = {}
            comittee = committee.text
            result = re.compile(r'(?<=\()(.+?)(?=\))').search(comittee)
            committee_role = 'Member'
            if result != None:
                committee_role = result.group(1)
                committee_name = comittee[:comittee.find('(')].strip()
            else:
                committee_name = comittee
            committee_dict['role'] = committee_role
            committee_dict['committee'] = committee_name
            committees_lst.append(committee_dict)

        contact_information_content = content.find(
            'div', 'field field-name-field-legis-person field-type-node-reference field-label-hidden')

        address = contact_information_content.find('div', 'adr')
        street = ''
        area_served = ''
        region = ''
        postal_code = ''
        if address != None:
            street = address.find('div', 'street-address').text.strip()
            area_served = address.find('span', 'locality').text
            region = address.find('span', 'region').text
            postal_code = address.find('span', 'postal-code').text
        else:
            print('Missing address: ' + str(GOVERLYTIC_ID))
        complete_address = street + ' ' + area_served + ', ' + region + ' ' + postal_code
        area_served = {area_served}
        numbers = contact_information_content.find_all(
            'div', re.compile('panel.*phone.*'))
        contact_information_lst = []
        for phone_number in numbers:
            contact_information = {}
            number_type = phone_number.find(
                'div', 'field-label').text.split(':')[0]
            number = phone_number.find('div', 'field-item even').text
            contact_information['number'] = number
            contact_information['type'] = number_type
            contact_information_lst.append(contact_information)

        email_content = contact_information_content.find(
            'div', re.compile('panel.*email.*'))

        email = email_content.find('a').text

        legislator_info.append(str(date_collected))
        legislator_info.append(GOVERLYTIC_ID)
        legislator_info.append(state_url)
        legislator_info.append(URL_TEMPLATE + str(GOVERLYTIC_ID))
        legislator_info.append(full_name)
        legislator_info.append(last_name)
        legislator_info.append(first_name)
        legislator_info.append(middle_name)
        legislator_info.append(suffix)
        legislator_info.append(COUNTRY_ID)
        legislator_info.append(COUNTRY)
        legislator_info.append(STATE_ID)
        legislator_info.append(STATE)
        legislator_info.append(PARTY_ID)
        legislator_info.append(legislator_party)
        legislator_info.append(district_number)
        legislator_info.append(role)
        legislator_info.append(committees_lst)
        legislator_info.append(area_served)
        legislator_info.append(contact_information_lst)
        legislator_info.append(complete_address)
        legislator_info.append(email)
        legislator_info.append(BIRTHDAY_NONE)
        legislator_info.append(MILITARY_EXPERIENCE_NONE)
        legislator_info.append(occupation)
        legislator_info.append(current_term)
        legislator_info.append(years_active)
        GOVERLYTIC_ID = GOVERLYTIC_ID + 1
        data.append(legislator_info)

        if VERBOSE:
            print('\n\n\n Result so far\n')

            print(date_collected)
            print(GOVERLYTIC_ID)
            pprint(lst_href[3]['href'])
            print(URL_TEMPLATE + str(GOVERLYTIC_ID))
            pprint(full_name)
            pprint(last_name)
            pprint(first_name)
            pprint(middle_name)
            pprint(suffix)
            print(COUNTRY_ID)
            print(COUNTRY)
            print(STATE_ID)
            print(STATE)
            print(PARTY_ID)
            pprint(legislator_party)
            pprint(district_number)
            pprint(role)
            pprint(committees_lst)
            print(area_served)
            pprint(contact_information_lst)
            pprint(complete_address)
            pprint(email)
            pprint(BIRTHDAY_NONE)
            pprint(MILITARY_EXPERIENCE_NONE)
            pprint(occupation)
            print(current_term)
            pprint(years_active)
    return data



data = retrieve_information(legislators_list)
pprint(len(legislators_list))
pprint(len(data))