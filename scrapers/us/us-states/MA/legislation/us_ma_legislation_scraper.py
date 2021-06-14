import sys
import os
from pathlib import Path
from datetime import datetime
from nameparser import HumanName
from multiprocessing import Pool
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

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
    # time.sleep(5)
    # type_check_boxes = browser.find_elements_by_class_name("refinerGroup")[5]
    # amendment = type_check_boxes.find_elements_by_tag_name("input")[0]
    # bill = type_check_boxes.find_elements_by_tag_name("input")[1]
    # resolution = type_check_boxes.find_elements_by_tag_name("input")[9]
    # amendment.click()
    # bill.click()
    # resolution.click()


    #while True:
    for i in range(0, 5):
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


def get_full_name(url):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    main_div = soup.find('div', {'id': 'main'})
    name = main_div.find('h1').text

    if "Senator" in name:
        name = name.split("Senator ")[1]
    elif "Representative" in name:
        name = name.split("Representative ")[1]
    if " - " in name:
        name = name.split(" - ")[0]
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
    committee = sponsor.text
    chamber = ''
    if "Joint" in committee:
        chamber = "joint"
    else:
        link = "http://www.kslegislature.org" + sponsor.get('href')
        page = scraper_utils.request(link)
        soup = BeautifulSoup(page.content, 'lxml')
        # getting sidebar on committee page
        sidebar = soup.find('div', {'id': 'sidebar'})
        list_items = sidebar.find_all('li')

        for item in list_items:
            if "House" in item:
                chamber = "house"
            elif "Senate" in item:
                chamber = "senate"
    committee_detail = {"chamber": chamber, "committee": committee}
    return committee_detail


def get_sponsor_id(sponsor_name):
    sponsor_id = None
    if "Committee" not in sponsor_name:
        hn = HumanName(sponsor_name)
        name_first = hn.first
        name_middle = hn.middle
        name_last = hn.last
        name_suffix = hn.suffix

        search_for = dict(name_last=name_last, name_first=name_first)
        sponsor_id = scraper_utils.get_legislator_id(**search_for)

    return sponsor_id


def get_sponsors(sidebar, row):
    sponsors = []
    sponsor_ids = []
    committees = []
    sponsors_tabs = sidebar.find(
        'ul', {'class': 'sponsor-tab-content'})
    try:
        sponsors_detail = sponsors_tabs.find_all('a')

        for sponsor in sponsors_detail:
            name = sponsor.text
            if "Committee" in name:
                committee = get_committees(sponsor)
                committees.append(committee)
                sponsors.append(name)
            else:
                link = "http://www.kslegislature.org" + sponsor.get('href')
                name = get_full_name(link)
                sponsor_id = get_sponsor_id(name)

                hn = HumanName(name)
                name_last = hn.last
                sponsors.append(name_last)
                if sponsor_id is not None:
                    sponsor_ids.append(sponsor_id)

    except Exception:
        pass

    row.committees = committees
    row.sponsors = sponsors
    row.sponsors_id = sponsor_ids


def get_principal_sponsor(sidebar, row):
    committees = []
    sponsors_tabs = sidebar.find(
        'ul', {'class': 'introduce-tab-content'})

    try:
        sponsors = sponsors_tabs.find_all('a')
        if len(sponsors) == 1:
            sponsor = sponsors[0]
            name = sponsor.text

            if "Committee" in name:
                committee = get_committees(sponsor)
                committees.append(committee)
                row.principal_sponsor = name
            else:
                link = "http://www.kslegislature.org" + sponsor.get('href')
                name = get_full_name(link)
                sponsor_id = get_sponsor_id(name)
                hn = HumanName(name)
                name_last = hn.last
                row.principal_sponsor = name_last
                row.principal_sponsor_id = sponsor_id

    except Exception:
        pass
    row.committees = committees


def get_bill_description(main_div, row):
    description = main_div.find('div', {'class': 'container'}).text
    description = description.split('Short Title')[1]

    if "(more)" in description:
        description = description[:description.index("(more)")]
    elif "Summary of Legislation" in description:
        description = description[:description.index("Summary of Legislation")]
    elif "View Testimony" in description:
        description = description[:description.index("View")]

    description = description.strip()

    row.bill_description = description


def get_introduced_date(bottom_div, row):
    table = bottom_div.find('table', {'class': 'bottom'})
    table_rows = table.findAll('tr')
    date_row = table_rows[-1]
    date = date_row.findAll('td')[0].text
    datetime_date = datetime.strptime(date, '%a, %b %d, %Y')
    datetime_date = datetime_date.strftime("%Y-%m-%d")
    row.date_introduced = datetime_date


def get_current_status(bottom_div, row):
    table = bottom_div.find('table', {'class': 'bottom'})
    table_rows = table.findAll('tr')
    current_status_row = table_rows[1]
    status = current_status_row.findAll('td')[2].text.strip()
    status = status.replace("  ", "")
    status = status.replace("\n", "")
    row.current_status = status


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
        row.actions = actions_list
    except Exception:
        pass


def get_voter_details_support_func(vote, name):
    voter_id = get_sponsor_id(name)
    hn = HumanName(name)
    name_last = hn.last
    voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
    return voter_data


def get_voter_details(main_content, voter_data_list, yeas, nays, pandp, anv, nv):
    all_names = []
    legislator_links = main_content.findAll('a')
    for link in legislator_links:
        link = 'http://www.kslegislature.org' + link.get('href')
        if "rep_" in link:
            name = get_full_name(link)
            all_names.append(name)
        elif "sen_" in link:
            name = get_full_name(link)
            all_names.append(name)
    group_one = yeas + nays
    group_two = yeas + nays + pandp
    group_three = yeas + nays + pandp + anv
    group_four = yeas + nays + pandp + anv + nv

    for name in all_names[: yeas]:
        vote = "yea"
        voter_data = get_voter_details_support_func(vote, name)
        voter_data_list.append(voter_data)

    for name in all_names[yeas: group_one]:
        vote = "nay"
        voter_data = get_voter_details_support_func(vote, name)
        voter_data_list.append(voter_data)

    for name in all_names[group_one: group_two]:
        vote = "passing"
        voter_data = get_voter_details_support_func(vote, name)
        voter_data_list.append(voter_data)

    for name in all_names[group_two: group_three]:
        vote = "absent"
        voter_data = get_voter_details_support_func(vote, name)
        voter_data_list.append(voter_data)

    for name in all_names[group_three: group_four]:
        vote = "not voting"
        voter_data = get_voter_details_support_func(vote, name)
        voter_data_list.append(voter_data)

    return voter_data_list


def get_vote_detail(votes, row):
    page = scraper_utils.request(votes)
    soup = BeautifulSoup(page.content, 'lxml')

    voter_data_list = []
    passed = 0

    bill_summary = soup.find('h4').text
    row.bill_summary = bill_summary

    main_content = soup.find('div', {'id': 'main_content'})
    getting_votes = main_content.findAll('h3')

    # getting if vote passed 1 or 0
    if getting_votes[0].text.lower() in {'passed', 'adopted'}:
        passed = 1

    # getting vote date
    date_string = getting_votes[0].text.split(' - ')[3]
    date_string = date_string.strip()
    date = datetime.strptime(date_string, '%m/%d/%Y')
    date = date.strftime("%Y-%m-%d")

    # getting Chamber
    chamber = getting_votes[0].text.split(' - ')[0]

    # getting description
    description = getting_votes[0].text.split(chamber + ' - ')[1]
    description = description[:description.index(";")]

    # getting number of Yea
    yeas = getting_votes[2].text.split("Yea - (")[1]
    yeas = yeas.split(')')[0]
    yeas = int(yeas)

    # getting number of Nay
    nays = getting_votes[3].text.split("Nay - (")[1]
    nays = nays.split(')')[0]
    nays = int(nays)

    # getting number of Present and passing
    pandp = getting_votes[4].text.split("Present and Passing - (")[1]
    pandp = pandp.split(')')[0]
    pandp = int(pandp)

    # getting number of absent not voting
    anv = getting_votes[5].text.split("Absent and Not Voting - (")[1]
    anv = anv.split(')')[0]
    anv = int(anv)

    # getting number of Not voting
    nv = getting_votes[6].text.split("Not Voting - (")[1]
    nv = nv.split(')')[0]
    nv = int(nv)

    total = yeas + nays + pandp + anv + nv

    voter_data_list = get_voter_details(main_content, voter_data_list, yeas, nays, pandp, anv, nv)

    vote_data = {'date': date, 'description': description,
                 'yea': yeas, 'nay': nays, 'nv': nv, 'absent': anv, 'total': total, 'passed': passed,
                 'chamber': chamber,
                 'votes': voter_data_list}

    return vote_data


def get_vote_data(soup, row):
    print("vote data")
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

        for r in table_rows:
            row_sections = r.findAll('td')
            print(row_sections)
            description = row_sections[2]
            print(description.text)
            if "Roll call" in description:
                print('role')
                vote_data_link = row_sections[2].find('a').get('href')
                print(vote_data_link)
            elif "YEA" in description:
                vote_data_link = row_sections[2].find('a').get('href')
                print(vote_data_link)
    except:
        pass
    # row.votes = voting_data


def get_bill_text(main_div, row):
    table_row = main_div.findAll('tr')
    try:
        pdf_link = "http://www.kslegislature.org" + table_row[1].find('a').get('href')
        response = requests.get(pdf_link, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        text = ""
        for page in pages:
            page_text = page.extract_text()
            text += page_text
        row.bill_text = text
    except Exception:
        row.bill_text = ""


def scrape(url):
    row = scraper_utils.initialize_row()

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    row.source_url = url
    session = get_session(soup, row)
    bill_name = get_bill_name(soup, row)

    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id

    # get_sponsors(sidebar, row)
    # get_principal_sponsor(sidebar, row)
    # get_bill_description(main_div, row)
    # get_introduced_date(bottom_div, row)
    # get_current_status(bottom_div, row)
    get_actions(soup, row)
    get_vote_data(soup, row)
    # get_bill_text(soup, row)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()

    data = [scrape(url) for url in urls]

    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    # data = clear_none_value_rows(data)
    # big_list_of_dicts = clear_none_value_rows(data)

    # scraper_utils.write_data(big_list_of_dicts)

    print('Complete!')
