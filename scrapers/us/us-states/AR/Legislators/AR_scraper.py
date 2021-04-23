'''
Author: Avery Quan
Date Created: March 2, 2021
Description: Scrapes data for the legislators of Arkansas using the Arkansas state website and wikipedia

Notes:
- Potential future errors for years active, site does not always show always show 2019 as a year served, so I guessed
    that the last year mentioned is from that year to current year. I also don't include year ranges because I don't think its that 
    relevant to years spent in office.

- Missing years served, could be scraped from within a paragraph on the individual house and senate sites, but since this would rely on 
    the paragraph word structure, it is highly subject to error in the future.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import pandas as pd
import requests
import boto3
import re
import datetime
from nameparser import HumanName
from pprint import pprint
import configparser
from database import Database
from multiprocessing.dummy import Pool
from bs4 import BeautifulSoup
from legislator_scraper_utils import USStateLegislatorScraperUtils



# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

# state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
# database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
# country = str(configParser.get('scraperConfig', 'country'))

# scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name, country)
scraper_utils = USStateLegislatorScraperUtils('AR', 'us_ar_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.arkleg.state.ar.us')
session_id = ''


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = {}

    # Logic goes here! Some sample code:
    scrape_url = 'https://www.arkleg.state.ar.us/Legislators/List'
    base_url = 'https://www.arkleg.state.ar.us'
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    chambers = soup.find_all(
        'div', class_='col-sm-2 col-md-2 d-none d-md-block d-lg-block d-xl-block')
    districts = soup.find_all('div', {
                              'class': 'col-md-2 col-md-2 d-none d-md-block d-lg-block d-xl-block', 'aria-colindex': 4})

    divs = soup.find_all('div', class_='col-sm-6 col-md-6')
    links = []
    for div in divs:
        links.append(div.find('a'))

    for index, path in enumerate(links):
        chamber = chambers[index].text.split('\n')[2].strip()
        district = districts[index].text.split('\n')[2].strip()
        urls[(chamber, district)] = base_url + path['href']

    return urls


def get_wiki_links(link, chamber):
    wikipedia_link = 'https://en.wikipedia.org'

    member_request = scraper_utils.request(link)
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    members = member_soup.find('table', class_='wikitable sortable')
    members = members.find_all('tr')[1:]

    links = {}

    for member in members:

        elements = member.find_all('td')
        district = elements[0].text.strip()
        member_url = elements[1].find('a')['href']

        links[(chamber, district)] = (wikipedia_link + member_url)
    scraper_utils.crawl_delay(crawl_delay)
    return links


def scrape_wiki(url, row):
    member_request = scraper_utils.request(url)
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    table = member_soup.find('table', class_='infobox vcard')
    if table is None:
        return

    panda = pd.read_html(str(table))[0]
    panda = panda.dropna()

    try:
        panda.columns = {'Key', 'Value'}
    except Exception as e:
        pass

    key = panda.columns[0]

    try:
        # gets cell for the year legislator assumed office from a dataframe, then extracts the year from the string in that cell
        years_active = re.sub('\[.*\]', '', panda[panda[key].str.contains(
            'Assumed office', regex=False)]['Key'].values[0])[-4:]
        # creates a list for the range of years active
        years_active = list(set(
            years_active + list(range(int(years_active), datetime.date.today().year + 1))))
        row.years_active = years_active
    except Exception as e:
        pass

    try:
        years = panda[panda[key].str.contains(
            'In office', regex=False)]['Key'].values

        for year in years:
            year_range = re.findall('\d\d\d\d', year)
            # creates a list for the range of years active
            years_active = list(
                range(int(year_range[0]), int(year_range[1]) + 1))
            row.years_active = list(set(years_active + row.years_active))

    except Exception as e:
        pass

    try:
        rep_birth = table.find('span', {"class": "bday"}).text
        birthday = datetime.datetime.strptime(rep_birth, "%Y-%m-%d").date()
        row.birthday = birthday
    except Exception as e:
        try:
            # sometimes the bday tag isnt there on some pages so we use a lambda, slower
            rep_birth = table.find(
                lambda tag: tag.name == "tr" and "Born" in tag.text).text
            year_birth = re.findall('\d\d\d\d', rep_birth)[0]
            birthday = datetime.datetime.strptime(year_birth, "%Y").date()
            row.birthday = birthday
        except Exception as e:
            pass

    try:
        occupation = table.find(lambda tag: tag.name ==
                                "tr" and "Profession" in tag.text).text
        occupation = occupation.replace('occupation', '')
        occupation = occupation.replace('Profession', '')
        row.occupation = occupation.split(',')
    except Exception as e:
        pass

    try:
        lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD",
                "DDS", "MS", "BS", "MBA", "MS", "MD"]
        reps = member_soup.find("div", {"class": "mw-parser-output"})
        left_column_tags = reps.findAll()
        lefttag = left_column_tags[0]
        for lefttag in left_column_tags:
            if lefttag.text == "Alma mater" or lefttag.text == "Education":
                index = left_column_tags.index(lefttag) + 1
                next = left_column_tags[index]
                # find text lines with either University, College, School
                alines = next.findAll(lambda tag: "University" in tag.text or "College" in tag.text or "School" in tag.text or ')' in tag.text or re.sub(
                    '[^a-zA-Z]+', "", tag.text) in lvls)
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
                        edinfo = {'level': level,
                                  'field': "", 'school': school}
                        row.education = edinfo
    except Exception as e:
        pass
    scraper_utils.crawl_delay(crawl_delay)
    return row


def scrape(urls):
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
    url = urls[0]

    row = scraper_utils.initialize_row()

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    row.source_url = url
    title = soup.find('h1')
    address = title.parent.find('b').string
    row.addresses.append({'location': 'district office', 'address': address})
    title = title.text.split(' ')
    row.role = title[0]
    name = HumanName(' '.join(title[1:-1]))
    row.name_full = name.full_name
    row.name_first = name.first
    row.name_last = name.last
    row.name_middle = name.middle
    row.name_suffix = name.suffix
    row.most_recent_term_id = session_id
    party = {'(D)': 'Democrat', '(R)': 'Republican', '(I)': 'Independent'}
    row.party = party[title[-1]]
    row.party_id = scraper_utils.get_party_id(row.party)

    table = soup.find('div', class_='col-md-7')
    trs = table.find_all(
        'div', class_='col-sm-12 d-sm-block d-md-none d-lg-none d-xl-none')

    for tr in trs:
        label = tr.find('b')
        try:
            data = tr.find('a').text
        except:
            data = tr.find('b').next_sibling

        if data is None:
            continue

        label = label.text
        data = data.strip()

        if label == 'Phone:':
            row.phone_number.append({'district office': data.replace(
                '(', '').replace(')', '').replace(' ', '-')})

        elif label == 'Email:':
            row.email = data

        elif label == 'District:':
            row.district = data

        elif label == 'Seniority:':
            row.seniority = int(data)

        elif label == 'Occupation:':
            row.occupation.append(data)

        elif label == 'Veteran:':
            row.military_experience = data

        elif label == 'Seniority:':
            row.seniority = data

        elif label == 'Public Service:':
            # We do not look at years ranges, what the years represent is too hard to figure out
            years = re.sub('\d\d\d\d-\d\d\d\d', '', data)
            years = re.findall('\d\d\d\d', data)
            year_range = []
            for year in years:
                if int(year) % 2 == 1:
                    year_range.append(int(year) + 1)
                year_range.append(int(year))

            # this has potential for error, I took a guess that the last year continues to current date
            year_range += list(range(year_range[-1],
                               datetime.datetime.now().year + 1))
            row.years_active = list(set(year_range))

    committees = soup.find_all('div', class_=['col-sm-12 col-md-12'])[1:]
    for committee in committees:
        group = committee.find('a').text.strip().title()

        try:
            role = committee.find('b').text.strip()
        except:
            role = 'Member'

        row.committees.append({'role:': role, 'committee:': group})

    scrape_wiki(urls[1], row)
    scraper_utils.crawl_delay(crawl_delay)
    return row


def set_session_id():
    scrape_url = 'https://www.arkleg.state.ar.us/Legislators/List'
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    scraper_utils.crawl_delay(crawl_delay)
    return(soup.find('div', class_='siteBienniumSessionName').text.split('-')[1].strip())


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    url = get_urls()
    wiki_url = get_wiki_links(
        'https://en.wikipedia.org/wiki/Arkansas_House_of_Representatives', 'House')
    wiki_url = {
        **wiki_url, **get_wiki_links('https://en.wikipedia.org/wiki/Arkansas_Senate', 'Senate')}

    urls = [(path, wiki_url[key])for key, path in url.items()]
    session_id = set_session_id()
    print('Initialized Scraping')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)
        # pprint(data)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')


# scrape_wiki('https://en.wikipedia.org/w/index.php?title=Jim_Wooten_(politician)&action=edit&redlink=1', scraper_utils.initialize_row())
