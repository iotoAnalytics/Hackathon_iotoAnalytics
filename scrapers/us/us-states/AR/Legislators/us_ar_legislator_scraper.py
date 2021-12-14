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

- Scrapes historical legislators by using full name as a key, will overwrite earlier legislators if they have the same name
- May also have duplicate legislators if they switch districts or chambers
'''

import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
print(os.path.dirname(os.path.realpath(__file__)))

from scraper_utils import USStateLegislatorScraperUtils
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
import configparser
from nameparser import HumanName
from datetime import datetime
import re
import pandas as pd
import sys
sys.setrecursionlimit(4000)

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


# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

# state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
# database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
# country = str(configParser.get('scraperConfig', 'country'))

# scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name, country)
scraper_utils = USStateLegislatorScraperUtils('AR', 'us_ar_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.arkleg.state.ar.us')
# session_id = ''

def get_sessions(historical=False):
    current_year = datetime.year
    session_url = f'https://www.arkleg.state.ar.us/Acts/SearchByRange?startAct=1&endAct=1000&keywords=&ddBienniumSession={current_year}%2F{current_year}R#SearchResults'

    page = scraper_utils.request(session_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    sessions = soup.find(
        'select', id='ddBienniumSession').find_all('option')[1:]

    if not historical:
        year, code = sessions[0]['value'].split('/')
        return {'year': year, 'code' :code}

    codes = []
    for session in sessions:
        year, code = session['value'].split('/')
        codes.append({'year': year, 'code' :code})
    return codes

def get_urls(historical=False):
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    base_url = 'https://www.arkleg.state.ar.us'
    urls = {}
    codes = get_sessions(historical)


    for session in codes:
        year = session['year']
        code = session['code']
        scrape_url = f'https://www.arkleg.state.ar.us/Legislators/List?ddBienniumSession={year}%2F{code}'
        
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
            chamber = chambers[index].text.split('\n')[2].strip().replace('Representative-Elect', 'Representative')
            district = districts[index].text.split('\n')[2].strip()
            name = path.text.split(' ')
            name = [name[0], name[-1]]
            if year == str(datetime.now().year): #non historical
                if (chamber, district, path.text) not in urls.keys():
                    urls[(chamber, district, path.text)] = (base_url + path['href'], year)
            else: #historical
                if path.text not in urls.keys():
                    urls[path.text] = (base_url + path['href'], year)
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


def get_wiki_links(link, chamber):
    wikipedia_link = 'https://en.wikipedia.org'

    member_request = scraper_utils.request(link)
    member_soup = BeautifulSoup(member_request.content, 'html.parser')
    members = member_soup.find('table', class_='wikitable sortable')
    members = members.find_all('tr')[1:]

    links = {}

    for member in members:
        try:
            elements = member.find_all('td')
            district = elements[0].text.strip()
            member_url = elements[1].find('a')['href']
        except:
            pass

        links[(chamber, district)] = (wikipedia_link + member_url)
    scraper_utils.crawl_delay(crawl_delay)
    return links


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Arkansas_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Arkansas_State_Senate"

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
        birthday = datetime.strptime(rep_birth, "%Y-%m-%d").date()
        row.birthday = birthday
    except Exception as e:
        try:
            # sometimes the bday tag isnt there on some pages so we use a lambda, slower
            rep_birth = table.find(
                lambda tag: tag.name == "tr" and "Born" in tag.text).text
            year_birth = re.findall('\d\d\d\d', rep_birth)[0]
            birthday = datetime.strptime(year_birth, "%Y").date()
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
    print(urls)
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
    try:
        url = urls[0][0]
        print(url)

        row = scraper_utils.initialize_row()

        page = scraper_utils.request(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        row.source_url = url
        title = soup.find('h1')
        try:
            address = title.parent.find('b').string
            row.addresses.append({'location': 'district office', 'address': address})
        except AttributeError:
            pass
        title = title.text.split(' ')
        row.role = title[0]
        party = {'(D)': 'Democrat', '(R)': 'Republican', '(I)': 'Independent'}
        if title[-1] in party.keys():
            row.party = party[title[-1]]
            row.party_id = scraper_utils.get_party_id(row.party)
            name = HumanName(' '.join(title[1:-1]))
        else:
            name = HumanName(' '.join(title[1:]))
        row.name_full = name.full_name
        row.name_first = name.first
        row.name_last = name.last
        row.name_middle = name.middle
        row.name_suffix = name.suffix
        row.most_recent_term_id = urls[0][1]

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
                row.phone_numbers.append({'district office': data.replace(
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
                try:
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
                                    datetime.now().year + 1))
                    row.years_active = list(set(year_range))
                except IndexError:
                    # no house/senate served prob
                    pass

        committees = soup.find_all('div', class_=['col-sm-12 col-md-12'])[1:]
        for committee in committees:
            group = committee.find('a').text.strip().title()

            try:
                role = committee.find('b').text.strip()
            except:
                role = 'Member'

            row.committees.append({'role:': role, 'committee:': group})

        try:
            scrape_wiki(urls[1], row)
        except:
            pass
        try:
            row.wiki_url = get_wiki_url(row)
        except:
            pass

        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender


        scraper_utils.crawl_delay(crawl_delay)
    except Exception as e:
        import traceback
        traceback.print_exc()
        
    return row


# def set_session_id():
#     scrape_url = 'https://www.arkleg.state.ar.us/Legislators/List'
#     page = scraper_utils.request(scrape_url)
#     soup = BeautifulSoup(page.content, 'html.parser')
#     scraper_utils.crawl_delay(crawl_delay)
#     return(soup.find('div', class_='siteBienniumSessionName').text.split('-')[1].strip())


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    url = get_urls(True)
    wiki_url = get_wiki_links(
        'https://en.wikipedia.org/wiki/Arkansas_House_of_Representatives', 'House')
    wiki_url = {
        **wiki_url, **get_wiki_links('https://en.wikipedia.org/wiki/Arkansas_Senate', 'Senate')}

    urls = []
    for key, path in url.items():
        try:
            urls.append([path, wiki_url[key[0:2]]])
        except KeyError:
            urls.append([path])

    # session_id = set_session_id()
    print('Initialized Scraping')

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    data = [scrape(url) for url in urls]

    # with Pool() as pool:
    #     data = pool.map(scrape, urls)
    leg_df = pd.DataFrame(data[:50])
    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Arkansas_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Arkansas_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    # with Pool() as pool:
    #     wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_data = [scraper_utils.scrape_ballotpedia_bio(link) for link in all_wiki_links]
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    print('Scraping complete')

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print(f'Scraper ran successfully!')
