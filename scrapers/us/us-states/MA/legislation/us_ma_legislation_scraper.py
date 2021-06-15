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
    chamber = ''
    if "Joint" in sponsor:
        chamber = "joint"
    elif "House" in sponsor:
        chamber = "house"
    elif "Senate" in sponsor:
        chamber = "senate"
    committee_detail = {"chamber": chamber, "committee": sponsor}
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


def get_sponsors(soup, row):
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


def get_principal_sponsor(soup, row):
    is_sponsor = ""
    principal_sponsor = ""
    try:
        is_sponsor = soup.find("dt").text
        principal_sponsor = soup.find("dd").text
    except:
        pass

    if "Sponsor" in is_sponsor:
        if "Committee" in principal_sponsor:
            committee_info = get_committees(principal_sponsor)
            row.principal_sponsor = principal_sponsor
            row.committees = committee_info
            print(principal_sponsor)
        else:
            try:
                sponsor_id = get_sponsor_id(principal_sponsor)
                hn = HumanName(principal_sponsor)
                name_last = hn.last
                row.principal_sponsor = name_last
                row.principal_sponsor_id = sponsor_id
                print(name_last)
            except:
                row.principal_sponsor = principal_sponsor
                print(principal_sponsor)
    print()


def get_bill_description(soup, row):
    try:
        description = soup.find("div", {"class": "col-xs-12 col-md-8"})
        description = description.find("h2")
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


def get_vote_detail(vote_links, row):
    for link in vote_links:
        url = link[0]
        pdf_link = base_url + url

        response = requests.get(pdf_link, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        pages = pdf.pages
        text = ""
        for page in pages:
            page_text = page.extract_text()
            text += page_text
        print(text)


    # voter_data_list = get_voter_details(main_content, voter_data_list, yeas, nays, pandp, anv, nv)
    #
    # vote_data = {'date': date, 'description': description,
    #              'yea': yeas, 'nay': nays, 'nv': nv, 'absent': anv, 'total': total, 'passed': passed,
    #              'chamber': chamber,
    #              'votes': voter_data_list}
    #
    # return vote_data


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
                votes = [vote_data_link, datetime_date, chamber]
                vote_links.append(votes)
    except:
        pass
    get_vote_detail(vote_links, row)
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

    #get_sponsors(soup, row)
    get_principal_sponsor(soup, row)
    get_bill_description(soup, row)
    get_bill_summary(soup, row)
    get_actions(soup, row)

    #get_vote_data(soup, row)
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
