import sys
import os
from database import CursorFromConnectionFromPool
from pathlib import Path
from datetime import datetime
from nameparser import HumanName
from multiprocessing import Pool
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from scraper_utils import USStateLegislatorScraperUtils
from scraper_utils import USStateLegislationScraperUtils
import pdfplumber
import requests
import io
import re

import dateutil.parser as dparser
from selenium import webdriver
import time
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import pandas as pd

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

PATH = "../../../../../web_drivers/chrome_win_91.0.4472.19/chromedriver.exe"
browser = webdriver.Chrome(PATH)

state_abbreviation = 'MA'
database_table_name = 'us_ma_legislation'
legislator_table_name = 'us_ma_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://malegislature.gov'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_individual_urls(url):
    links = []
    browser.get(url)
    table = browser.find_element_by_id("searchTable")
    links_table = table.find_element_by_tag_name("tbody")
    rows = links_table.find_elements_by_tag_name('tr')
    for row in rows:
        row_elements = row.find_elements_by_tag_name("td")
        link = row_elements[1].find_element_by_tag_name('a').get_attribute('href')
        links.append(link)
    return links


def get_urls():
    urls = []
    path = '/Bills/Search'
    scrape_url = base_url + path

    browser.get(scrape_url)
    url = scrape_url
    time.sleep(5)

    amendment = browser.find_element_by_xpath('//*[@id="refiners"]/fieldset[6]/div[1]/div[1]/label/input')
    amendment.click()
    bill = browser.find_element_by_xpath('//*[@id="refiners"]/fieldset[6]/div[1]/div[2]/label/input')
    bill.click()
    more_options = browser.find_element_by_xpath('//*[@id="refiners"]/fieldset[6]/a')
    more_options.click()
    time.sleep(3)
    resolution = browser.find_element_by_xpath('//*[@id="lawsfilingtypeModal"]/div/div/div[2]/div[2]/div[11]/label/input')
    resolution.click()
    apply_filter = browser.find_element_by_xpath('//*[@id="lawsfilingtypeModal"]/div/div/div[3]/button[2]')
    apply_filter.click()
    time.sleep(3)
    url = browser.current_url

    #while True:
    for i in range(0, 262):
        try:
            urls += (get_individual_urls(url))
            pagination_table = browser.find_element_by_class_name("pagination")
            pagination_buttons = pagination_table.find_elements_by_tag_name('li')
            next_button_container = pagination_buttons[7]
            next_button = next_button_container.find_element_by_tag_name('a')
            next_button.click()
            url = browser.current_url
            browser.get(url)
        except ElementClickInterceptedException:
            break
    return urls


def get_legislator_urls():
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


def get_session(soup, row):
    session_text = soup.find('span', {'class': 'subTitle'}).text
    session = session_text.split(' ')[0].strip()
    session = re.findall(r'[0-9]', session)
    session = "".join(session)
    row.session = session

    return session


def get_bill_name(soup, row):
    name = soup.find('h1').text.strip()
    get_bill_type_chamber(name, row)
    name = name.split(" ")[1]
    name = name.replace(".", "")
    row.bill_name = name
    return name


def get_bill_type_chamber(bill_name, row):
    bill_type = bill_name.split(' ')[0]
    bill_name = bill_name.split(' ')[1]
    bill_name = bill_name.split('.')[0]
    chambers = {
        'H': 'House',
        'S': 'Senate',
        'HD': 'House',
        'SD': 'Senate',
    }
    chamber_origin = chambers.get(bill_name)
    row.chamber_origin = chamber_origin
    row.bill_type = bill_type


def get_committees(sponsor):
    chamber = ''
    if "Joint" in sponsor:
        chamber = "joint"
    elif "House" in sponsor:
        chamber = "house"
    elif "Senate" in sponsor:
        chamber = "senate"
    committee_detail = {"chamber": chamber, "committee": sponsor}
    return committee_detail


def get_legislator_id_by_full_name(name):
    legislator_id = None
    if "Committee" not in name:
        hn = HumanName(name)
        name_first = hn.first
        name_middle = hn.middle
        name_last = hn.last
        name_suffix = hn.suffix

        search_for = dict(name_last=name_last, name_first=name_first)

        legislator_id = scraper_utils.get_legislator_id(**search_for)

    return legislator_id


def get_legislator_id_by_last_name(name):
    legislator_id = None
    name_last = name
    if " " in name:
        name_last = name.split(" ")[1]
        first_initial = name.split(' ')[0]
        legislator_id = scraper_utils.legislators_search_startswith('goverlytics_id', "name_first",
                                                                 first_initial , name_last=name_last)
    else:
        search_for = dict(name_last=name_last)
        legislator_id = scraper_utils.get_legislator_id(**search_for)

    return legislator_id


# def get_sponsors(soup, row):
#     sponsors = []
#     sponsor_ids = []
#     committees = []
#     sponsors_tabs = sidebar.find(
#         'ul', {'class': 'sponsor-tab-content'})
#     try:
#         sponsors_detail = sponsors_tabs.find_all('a')
#
#         for sponsor in sponsors_detail:
#             name = sponsor.text
#             if "Committee" in name:
#                 committee = get_committees(sponsor)
#                 committees.append(committee)
#                 sponsors.append(name)
#             else:
#                 link = "http://www.kslegislature.org" + sponsor.get('href')
#                 name = get_full_name(link)
#                 sponsor_id = get_sponsor_id(name)
#
#                 hn = HumanName(name)
#                 name_last = hn.last
#                 sponsors.append(name_last)
#                 if sponsor_id is not None:
#                     sponsor_ids.append(sponsor_id)
#
#     except Exception:
#         pass
#
#     row.committees = committees
#     row.sponsors = sponsors
#     row.sponsors_id = sponsor_ids


# def get_principal_sponsor(soup, row):
#     is_sponsor = ""
#     principal_sponsor = ""
#     try:
#         is_sponsor = soup.find("dt").text
#         principal_sponsor = soup.find("dd").text
#     except:
#         pass
#
#     if "Sponsor" in is_sponsor:
#         if "Committee" in principal_sponsor:
#             committee_info = get_committees(principal_sponsor)
#             row.principal_sponsor = principal_sponsor
#             row.committees = committee_info
#             print(principal_sponsor)
#         else:
#             try:
#                 sponsor_id = get_sponsor_id(principal_sponsor)
#                 hn = HumanName(principal_sponsor)
#                 name_last = hn.last
#                 row.principal_sponsor = name_last
#                 row.principal_sponsor_id = sponsor_id
#                 print(name_last)
#             except:
#                 row.principal_sponsor = principal_sponsor
#                 print(principal_sponsor)
#     print()


def get_bill_description(soup, row):
    try:
        description = soup.find("div", {"class": "col-xs-12 col-md-8"})
        description = description.find("p", {'id': 'pinslip'}).text
        description = description.strip()
        row.bill_description = description
    except:
        pass


def get_bill_summary(soup, row):
    try:
        summary = soup.find("p", {"id": "pinslip"}).text
        summary = summary.strip()
        row.bill_summary = summary
    except:
        pass


def get_introduced_date(actions_list, row):
    if len(actions_list) > 0:
        oldest_action = actions_list[-1]
        date = oldest_action['date']
        row.date_introduced = date


def get_current_status(actions_list, row):
    if len(actions_list) > 0:
        most_recent_action = actions_list[0]
        current_status = most_recent_action['description']
        row.current_status = current_status


def get_actions(soup, row):
    actions_list = []
    tables = soup.findAll('table')
    for table in tables:
        row_names = table.find("thead")
        try:
            row_names = row_names.findAll("tr")[0].text
            if "Date" in row_names:
                table = table
        except Exception:
            pass

    try:
        table_rows = table.findAll('tr')

        for r in reversed(table_rows):
            row_sections = r.findAll('td')
            date = row_sections[0].text
            datetime_date = datetime.strptime(date, '%m/%d/%Y')
            datetime_date = datetime_date.strftime("%Y-%m-%d")
            chamber = row_sections[1].text
            description = row_sections[2].text.strip()
            if "\n" in description:
                description = description.replace("\n", "")
                description = description.replace("  ", "")
            row_data = {'date': datetime_date, 'action_by': chamber, 'description': description}
            actions_list.append(row_data)

    except Exception:
        pass
    get_current_status(actions_list, row)
    get_introduced_date(actions_list, row)
    row.actions = actions_list


def get_vote_detail(vote_links, row):
    for link in vote_links:
        url = link[0]
        date = link[1]
        chamber = link[2]
        description = link[3]
        pdf_link = base_url + url

        response = requests.get(pdf_link, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        text = ""
        for page in pages:
            page_text = page.extract_text()
            text += page_text

        vote_list = parse_vote_pdf_for_voter_info(text)
        count = parse_vote_pdf_for_vote_count(text)
        yea = count[0]
        nay = count[1]
        nv = count[2]
        passed = count[3]
        vote_detail = {"date": date, "description": description, "yea": yea, "nay": nay,
                       "nv": nv, "absent": None, "passed": passed, "chamber": chamber, "votes": vote_list}
        return vote_detail


def parse_vote_pdf_for_vote_count(text):
    if "MASSACHUSETTS HOUSE OF REPRESENTATIVES" in text:
        text = text.split('\n')
        votes_count_detail = text[3]
        votes_count_detail = votes_count_detail.split(' ')
        yeas = int(votes_count_detail[2])
        nays = int(votes_count_detail[4])
        not_voting = int(votes_count_detail[6])
        if yeas > 80:
            passed = 1
        else:
            passed = 0
    else:
        text = text.split(' − ')
        yeas = text[1]
        yeas = int(yeas.split(".")[0])
        nays = text[2]
        nays = int(nays.split(".")[0])
        not_voting = 0
        if yeas > 20:
            passed = 1
        else:
            passed = 0

    vote_count = [yeas, nays, not_voting, passed]

    return vote_count


def parse_vote_pdf_for_voter_info(text):
    list_of_yeas = []
    list_of_nays = []
    list_of_voters = []
    if "MASSACHUSETTS HOUSE OF REPRESENTATIVES" in text:
        list_of_votes = text.split('N/V\n')[1]
        list_of_votes = list_of_votes.replace('Mr. ', '')
        list_of_votes = list_of_votes.replace('\n', ' ')
        list_of_votes = list_of_votes.split(' ')
        count = 1
        for item in list_of_votes:
            vote_type = {
                'Y': "yea",
                'N': "nay",
                'P': "present",
                'X': "not voting"
            }
            vote = vote_type.get(item)
            if item in vote_type:

                try:
                    legislator_name = list_of_votes[count]
                    legislator_name = legislator_name.replace("--", '')
                    name = legislator_name
                    if ',' in legislator_name:
                        legislator_name = list_of_votes[count+1] + list_of_votes[count]
                        legislator_name = legislator_name.replace(',', '')
                        legislator_name = legislator_name.replace('.', ' ')
                except:
                    pass
                legislator_id = get_legislator_id_by_last_name(legislator_name)
                voter = {'goverlytics_id': legislator_id, 'legislator': name, 'votetext': vote}
                list_of_voters.append(voter)
            count += 1
    else:
        list_of_yeas = text.split("YEAS.")[1].strip()
        list_of_yeas = list_of_yeas.split("NAYS")[0]
        list_of_yeas = re.findall(r'([A-Za-z, ]+[A-Za-z]+)', list_of_yeas)

        try:
            list_of_nays = text.split("NAYS ")[1].strip()
            list_of_nays = list_of_nays.replace('− 0.', '')
            list_of_nays = re.findall(r'([A-Za-z, ]+[A-Za-z]+)', list_of_nays)
        except:
            pass
        for vote in list_of_yeas:
            vote_text = "yea"
            name = vote.split(",")

            try:
                firstname = name[1]
                firstname = firstname.split(' ')[1]
                lastname = name[0].strip()
                fullname = firstname + ' ' + lastname
            except:
                pass
            legislator_id = get_legislator_id_by_full_name(fullname)
            voter = {'goverlytics_id': legislator_id, 'legislator': lastname, 'votetext': vote_text}
            list_of_voters.append(voter)

        for vote in list_of_nays:
            vote_text = "nay"
            name = vote.split(",")

            try:
                firstname = name[1]
                firstname = firstname.split(' ')[1]
                lastname = name[0].strip()
                fullname = firstname + ' ' + lastname
            except:
                pass
            legislator_id = get_legislator_id_by_full_name(fullname)
            voter = {'goverlytics_id': legislator_id, 'legislator': lastname, 'votetext': vote_text}
            list_of_voters.append(voter)

        return list_of_voters


def get_vote_data(soup, row):
    vote_links = []
    vote_data_link = None
    tables = soup.findAll('table')
    for table in tables:
        row_names = table.find("thead")
        try:
            row_names = row_names.findAll("tr")[0].text
            if "Date" in row_names:
                table = table
        except Exception:
            pass

    try:
        table_body = table.find('tbody')
        table_rows = table_body.findAll('tr')

        for r in table_rows:
            row_sections = r.findAll('td')
            description = row_sections[2].text
            description = description.lower()
            date = row_sections[0].text
            datetime_date = datetime.strptime(date, '%m/%d/%Y')
            datetime_date = datetime_date.strftime("%Y-%m-%d")
            chamber = row_sections[1].text
            if "roll call" in description:
                vote_data_link = row_sections[2].find('a').get('href')
            if "yea" in description:
                vote_data_link = row_sections[2].find('a').get('href')
            if vote_data_link is not None:
                votes = [vote_data_link, datetime_date, chamber, description]
                vote_links.append(votes)
    except:
        pass

    voting_data = get_vote_detail(vote_links, row)
    row.votes = voting_data


def get_bill_text(url, row):
    pdf_url = url + ".pdf"

    try:
        response = requests.get(pdf_url, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        text = ""
        for page in pages:
            page_text = page.extract_text()
            text += page_text
        row.bill_text = text
    except:
        pass


def get_bill_title(soup, row):
    try:
        title = soup.find('div', {'class': 'col-xs-12 col-md-8'})
        title = title.find('h2').text
        row.bill_title = title
    except:
        pass


def scrape(url):
    row = scraper_utils.initialize_row()
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    row.source_url = url
    session = get_session(soup, row)
    bill_name = get_bill_name(soup, row)
    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id
    #get_sponsors(soup, row)
    # get_principal_sponsor(soup, row)
    get_bill_description(soup, row)
    get_bill_summary(soup, row)
    get_actions(soup, row)
    print(url)
    get_vote_data(soup, row)
    get_bill_text(url, row)
    get_bill_title(soup, row)
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


def get_legislator_id_by_full_name(soup):
    legislator_id = None
    name_block = soup.find('h1')
    try:
        role = name_block.find('span').text.strip()
        name_full = name_block.text.split(role)[1]
        if "Democrat" in name_full:
            name_full = name_full.split("Democrat")[0].strip()
        else:
            name_full = name_full.split("Republican")[0].strip()

        hn = HumanName(name_full)
        name_last = hn.last
        name_first = hn.first

    except Exception as e:
        print(e)

    try:
        search_for = dict(name_last=name_last, name_first=name_first)

        legislator_id = scraper_utils.get_legislator_id(**search_for)
    except Exception as e:
        print(e)

    if "'" in name_last:
        name_last = name_last.replace("'", "''")
    return legislator_id, name_last


def get_legislators_sponsored_bills(soup):
    sponsored_bills = []
    table_section = soup.find('div', {'class': 'tab-content'})
    try:
        rows = table_section.find_all('tr')
        for row in rows[1:]:
            columns = row.find_all('td')
            if '*' not in columns[3].text:
                bill = columns[1].find('a').get('href')
                bill = base_url + bill
                sponsored_bills.append(bill)
                print(bill)
    except Exception as e:
        print(e)
    return sponsored_bills


def get_legislators_cosponsored_bills(url):
    cosponsored_bills = []
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    table_section = soup.find('div', {'class': 'tab-content'})
    try:
        rows = table_section.find_all('tr')
        for row in rows[1:]:
            columns = row.find_all('td')
            if '*' not in columns[3].text:
                bill = columns[1].find('a').get('href')
                bill = base_url + bill
                cosponsored_bills.append(bill)
                print(bill)
    except Exception as e:
        print(e)
    print(cosponsored_bills)
    return cosponsored_bills


def update_legislation_table(legislator_data,
                             legislator_id,
                             last_name,
                             cur):
    print("updating")
    sponsored_bills = legislator_data[0]
    cosponsored_bills = legislator_data[1]
    for bill in sponsored_bills:
        if legislator_id is not None:
            try:
                query = (f"UPDATE us_ma_legislation SET sponsors_id = array_append(sponsors_id, '{legislator_id}'), sponsors = array_append(sponsors, '{last_name}') WHERE source_url = '{bill}' AND '{legislator_id}' != ALL(sponsors_id) AND '{last_name}' != ALL(sponsors);")
                print(query)
                cur.execute(query)
            except Exception as e:
                print(e)

    for bill in cosponsored_bills:
        if legislator_id is not None:
            try:
                query = (f"UPDATE us_ma_legislation SET cosponsors_id = array_append(cosponsors_id, '{legislator_id}'), cosponsors = array_append(cosponsors, '{last_name}') WHERE source_url = '{bill}' AND '{legislator_id}' != ALL(cosponsors_id) AND '{last_name}' != ALL(cosponsors);")
                print(query)
                cur.execute(query)
            except Exception as e:
                print(e)


def scrape_for_sponsors(url, cur):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    sponsored_bills = get_legislators_sponsored_bills(soup)
    try:
        term_title = soup.find('span', {'class': 'headNumber'}).text
        term_id = term_title.split(' ')[1].strip()
        term_id = re.findall(r'[0-9]', term_id)
        term_id = "".join(term_id)
        cosponsored_bills_url = url + "/" + term_id + "/Bills/Cosponsored"
        cosponsored_bills = get_legislators_cosponsored_bills(cosponsored_bills_url)
        legislator_id, last_name = get_legislator_id_by_full_name(soup)
        legislator_data = [sponsored_bills, cosponsored_bills]
        update_legislation_table(legislator_data, legislator_id, last_name, cur)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
   # urls = get_urls()

    # data = [scrape(url) for url in urls]

    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    #
    # scraper_utils.write_data(data)

    #sponsor data needs to be scraped from the legislator pages and added to the table.
    with CursorFromConnectionFromPool() as cur:
        for url in get_legislator_urls():
            scrape_for_sponsors(url, cur)

    print('Complete!')
