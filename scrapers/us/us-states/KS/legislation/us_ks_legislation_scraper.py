'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

import boto3
from datetime import datetime
from pprint import pprint
from urllib.parse import parse_qs
import urllib.parse as urlparse
import re
from nameparser import HumanName
import configparser
from database import Database
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
from scraper_utils import USStateLegislationScraperUtils

state_abbreviation = 'KS'
database_table_name = 'us_ks_legislation'
legislator_table_name = 'us_ks_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'http://www.kslegislature.org'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    paths = '/li/b2021_22/measures/bills/', '/li/b2021_22/measures/concurs/', '/li/b2021_22/measures/resos/'
    for path in paths:
        scrape_url = base_url + path
        page = scraper_utils.request(scrape_url)
        soup = BeautifulSoup(page.content, 'html.parser')

        table = soup.find(
            'div', {'class': 'infinite-tabs'})

        for li in table.findAll('li')[:100]:
            link = base_url + li.find('a').get('href')
            urls.append(link)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls


def get_bill_name(main_div):
    name = main_div.find('h1').text

    return name


def get_session(soup):
    header_div = soup.find('div', {'id': 'logo2'})
    session = header_div.find('h5').text
    session = session.split(' ')[0].strip()

    return session


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
    chamber_origin = ''
    bill_type = ''
    if 'HB' in bill_name:
        chamber_origin = 'House'
        bill_type = 'Bill'
    elif 'HR' in bill_name:
        chamber_origin = 'House'
        bill_type = 'Resolution'
    elif 'HCR' in bill_name:
        chamber_origin = 'House'
        bill_type = 'Concurrent Resolution'
    elif 'SB' in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Bill'
    elif 'SR' in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Resolution'
    elif 'SCR' in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Concurrent Resolution'

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
    sponsor_id = None
    sponsors_tabs = sidebar.find(
        'ul', {'class': 'sponsor-tab-content'})
    try:
        sponsors_detail = sponsors_tabs.find_all('a')

        for sponsor in sponsors_detail:
            name = sponsor.text
            if "Committee" in name:
                committee = get_committees(sponsor)
                committees.append(committee)
            else:
                link = "http://www.kslegislature.org" + sponsor.get('href')
                name = get_full_name(link)
                sponsor_id = get_sponsor_id(name)

                hn = HumanName(name)
                name_last = hn.last

            sponsors.append(name_last)
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
    sponsor_id = None
    try:
        sponsors = sponsors_tabs.find_all('a')
        if len(sponsors) == 1:
            sponsor = sponsors[0]
            name = sponsor.text

            if "Committee" in name:
                committee = get_committees(sponsor)
                committees.append(committee)
            else:
                link = "http://www.kslegislature.org" + sponsor.get('href')
                name = get_full_name(link)
                sponsor_id = get_sponsor_id(name)
                hn = HumanName(name)
                name_last = hn.last


    except Exception:
        pass
    row.committees = committees
    row.principal_sponsor_id = sponsor_id
    row.principal_sponsor = name_last


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
    date_row = table_rows[len(table_rows) - 1]
    date = date_row.findAll('td')[0].text
    datetime_date = datetime.strptime(date, '%a, %b %d, %Y')
    datetime_date = datetime_date.strftime("%Y-%m-%d")
    row.date_introduced = datetime_date


def get_current_status(bottom_div, row):
    table = bottom_div.find('table', {'class': 'bottom'})
    table_rows = table.findAll('tr')
    current_status_row = table_rows[1]
    status = current_status_row.findAll('td')[2].text.strip()
    row.current_status = status


def get_actions(bottom_div, row):
    actions_list = []
    table = bottom_div.find('table', {'class': 'bottom'})
    table_rows = table.findAll('tr')

    for row in table_rows[1:]:
        row_sections = row.findAll('td')

        date = row_sections[0].text
        datetime_date = datetime.strptime(date, '%a, %b %d, %Y')
        datetime_date = datetime_date.strftime("%Y-%m-%d")

        chamber = row_sections[1].text

        description = row_sections[2].text.strip()
        if "\n" in description:
            description = description.replace("\n", "")
            description = description.replace("  ", "")

        row_data = {'date': datetime_date, 'action_by': chamber, 'description': description}
        actions_list.append(row_data)
    row.actions = actions_list


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
        voter_id = get_sponsor_id(name)
        hn = HumanName(name)
        name_last = hn.last
        vote = "yea"
        voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
        voter_data_list.append(voter_data)

    for name in all_names[yeas: group_one]:
        voter_id = get_sponsor_id(name)
        hn = HumanName(name)
        name_last = hn.last
        vote = "nay"
        voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
        voter_data_list.append(voter_data)

    for name in all_names[group_one: group_two]:
        voter_id = get_sponsor_id(name)
        hn = HumanName(name)
        name_last = hn.last
        vote = "passing"
        voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
        voter_data_list.append(voter_data)

    for name in all_names[group_two: group_three]:
        voter_id = get_sponsor_id(name)
        hn = HumanName(name)
        name_last = hn.last
        vote = "absent"
        voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
        voter_data_list.append(voter_data)

    for name in all_names[group_three: group_four]:
        voter_id = get_sponsor_id(name)
        hn = HumanName(name)
        name_last = hn.last
        vote = "not voting"
        voter_data = {'goverlytics_id': voter_id, 'legislator': name_last, 'votetext': vote}
        voter_data_list.append(voter_data)

    print(voter_data_list)

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
    if "passed" in getting_votes[0].text:
        passed = 1
    elif "Passed" in getting_votes[0].text:
        passed = 1
    elif "Adopted" in getting_votes[0].text:
        passed = 1

    # getting vote date
    date_string = getting_votes[0].text.split(' - ')[3]
    date_string = date_string.strip()
    date = datetime.strptime(date_string, '%m/%d/%Y')
    date = date.strftime("%Y-%m-%d")

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

    voter_data_list = get_voter_details(main_content, voter_data_list, yeas, nays, pandp, anv, nv)

    # need to still get description and chamber
    vote_data = {'date': date, 'description': "On passage of the bill.",
                 'yea': yeas, 'nay': nays, 'nv': nv, 'absent': anv, 'total': 127, 'passed': passed, 'chamber': "House",
                 'votes': voter_data_list}


def get_vote_data(bottom_div, row):
    table = bottom_div.find('table', {'class': 'bottom'})
    table_rows = table.findAll('tr')
    for r in table_rows:
        r_text = r.text
        if "Final Action" in r_text:
            try:
                votes = r.find('a').get('href')
                votes = "http://www.kslegislature.org" + votes
                get_vote_detail(votes, row)
            except Exception:
                pass


def scrape(url):
    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')

    row.source_url = url

    # getting the main part of the page
    main_div = soup.find('div', {'id': 'main'})
    # getting sidebar
    sidebar = soup.find('div', {'id': 'sidebar'})
    # getting bottom div
    bottom_div = soup.find('div', {'id': 'full'})

    bill_name = get_bill_name(main_div)
    session = get_session(soup)

    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'

    row.goverlytics_id = goverlytics_id
    row.bill_name = bill_name
    row.session = session

    # get_bill_type_chamber(bill_name, row)
    # get_sponsors(sidebar, row)
    # get_principal_sponsor(sidebar, row)
    # get_bill_description(main_div, row)
    # get_introduced_date(bottom_div, row)
    # get_current_status(bottom_div, row)
    # get_actions(bottom_div, row)
    get_vote_data(bottom_div, row)

    # The Illinois state legislation website has their data stored in a weird way...
    # everything is stored in spans so we're gonna try pulling the data we need from
    # those. Your implementations will probably look quite a bit different than this.

    # Get bill description and summary
    # bill_description = ''
    # bill_summary = ''
    # spans = soup.findAll('span')
    # for idx, span in enumerate(spans):
    #     txt = span.text
    #     if 'Short Description:' in txt:
    #         bill_description = spans[idx + 1].text
    #     if 'Synopsis As Introduced' in txt:
    #         bill_summary = spans[idx + 1].text.strip()
    # row.bill_description = bill_description
    # row.bill_summary = bill_summary
    #
    # # Get bill sponsors
    # table = soup.find(
    #     'table', {'width': '440', 'border': '0', 'align': 'left'})
    # table_td = table.find('td', {'width': '100%'})
    #
    # a_tag = table_td.findAll('a', href=True)
    # sponsors = []
    # for a in a_tag:
    #     if '/house/Rep.asp' in a['href'] or '/senate/Senator.asp' in a['href']:
    #         sponsors.append(a.text)
    #
    # # # We'll now try to get the legislator goverlytics ID. Fortunately for us, this
    # # # site provides a unique identifier for each legislator. Normally we would do
    # # # the following:
    # # sponsor_id = scraper_utils.get_legislator_id(state_member_id=legislator_id)
    # # # However, since this is often not the case, we will search for the id using the
    # # # legislator name. We are given the legislator's full name, but if you are given
    # # # only the legislator initials and last name, which is more often the case, be sure to
    # # # use the legislators_search_startswith() method, which might look something like this:
    # # sponsor_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', first_initial, name_last=name_last)
    #
    # sponsors_id = []
    # for sponsor in sponsors:
    #     hn = HumanName(sponsor)
    #     name_first = hn.first
    #     name_middle = hn.middle
    #     name_last = hn.last
    #     name_suffix = hn.suffix
    #
    #     search_for = dict(name_first=name_first, name_middle=name_middle,
    #                       name_last=name_last, name_suffix=name_suffix)
    #
    #     sponsor_id = scraper_utils.get_legislator_id(**search_for)
    #
    #     # Some sponsor IDs weren't found, so we won't include these.
    #     # If you are unable to find legislators based on the provided search criteria, be
    #     # sure to investigate. Check the database and make sure things like names match
    #     # exactly, including case and diacritics.
    #     if sponsor_id is not None:
    #         sponsors_id.append(sponsor_id)
    #
    # row.sponsors = sponsors
    # row.sponsors_id = sponsors_id
    #
    # # Get actions
    # actions_table = soup.findAll('table', {
    #                              'width': '600', 'cellspacing': '0', 'cellpadding': '2', 'bordercolor': 'black', 'border': '1'})[1]
    #
    # action_date = ''
    # action_by = ''
    # action_description = ''
    # actions = []
    # number_of_columns = 3
    # # Skip the header row
    # for idx, td in enumerate(actions_table.findAll('td')[3:]):
    #     # With this type of method, normally you would search by 'tr' and then grab the value
    #     # from each 'td' in the row, but for some reason, beautiful soup wasn't able to find
    #     # the 'tr' so I had to get the value using a different, less intuitive method.
    #     mod = idx % number_of_columns
    #     if mod == 0:
    #         action_date = td.text.strip()
    #     if mod == 1:
    #         action_by = td.text.strip()
    #     if mod == 2:
    #         action_description = td.text.strip()
    #         actions.append(
    #             dict(date=action_date, action_by=action_by, description=action_description))
    #
    # # We can get the date introduced from the first action, and the current status from
    # # the most recent action.
    # date_introduced = None
    # current_status = ''
    # if len(actions) > 0:
    #     date_introduced = datetime.datetime.strptime(
    #         actions[0]['date'], '%m/%d/%Y')
    #     current_status = actions[-1]['description']
    #
    # row.actions = actions
    # row.current_status = current_status
    # row.date_introduced = date_introduced
    #
    # # There's more data on other pages we can collect, but we have enough data for this demo!
    #
    # # Delay so we do not overburden servers
    # scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')

    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    # scraper_utils.write_data(data)

    print('Complete!')
