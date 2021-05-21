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
import pdfplumber as pdfplumber

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3
from selenium import webdriver
from selenium.webdriver.support.select import Select
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
import pandas as pd
import pdfplumber
import requests
import io
import selenium



# Other import statements


state_abbreviation = 'LA'
database_table_name = 'us_la_legislation'
legislator_table_name = 'us_la_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://legis.la.gov/'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)
legislator_name_special_cases = ["Amedee", "Beryl Amedee", "Beryl A. Amedee", "Johnson,T.", "Pat Moore", "Ken Brass",
                                 "Samuel Jenkins", "Larry Bagley", "Larry Frieman", "Gabe Firment", "Ray Garofalo",
                                 "Daryl Adams", "Robert Carter","Scott McKnight", "Joe Stagni", "Beau Beaullieu",
                                 "Mike Johnson"]
legislator_name_special_cases_reference = ["Amedée", "Beryl Amedée", "Beryl A. Amedée", "Johnson,C.", "Patricia Moore",
                                           'Kendricks "Ken" Brass', "Sam L. Jenkins, Jr.", 'Lawrence A. "Larry" Bagley',
                                           'Lawrence "Larry" Frieman', 'Michael "Gabe" Firment',
                                           'Raymond E. Garofalo, Jr.', 'Roy Daryl Adams', 'Robby Carter',
                                           'Markham Scott McKnight', 'Joseph A. Stagni', 'Gerald "Beau" Beaullieu, IV',
                                           'Michael T. Johnson']


def get_urls():
    '''
    get urls for all types of bills.

    return: a list of urls of bills.
    '''
    urls = []
    WEBDRIVER_PATH = "D:\work\IOTO\goverlytics-scrapers\web_drivers\chrome_win_90.0.4430.24\chromedriver.exe"
    url = "https://legis.la.gov/legis/BillSearch.aspx?sid=LAST&e=P1"
    driver = webdriver.Chrome(WEBDRIVER_PATH)
    select_index = 0
    types_of_bills = 8

    driver.get(url)
    search_by_range_btn = driver.find_element_by_id("ctl00_ctl00_PageBody_PageContent_btnHeadRange")
    search_by_range_btn.click()
    sleep(2)

    while select_index < types_of_bills:
        bill_lists_prep_automation(driver, url, select_index)
        sleep(3)
        urls = scrape_urls(driver, urls)
        back_to_search = driver.find_element_by_xpath("//a[contains(text(), '< Back to Search')]")
        back_to_search.click()
        select_index += 1
        sleep(3)

    print(f"All urls collected, total {str(len(urls))} bill urls in list")

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)
    driver.quit()

    return urls


def scrape_urls(driver, urls):
    """
    scrape urls for each type of bills, and then append to the urls list.

    param: driver
    param: urls list
    return: urls list with added urls
    """
    while True:
        next_page = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), ' > ')]")))
        if next_page.get_attribute(
                "href") == "javascript:__doPostBack('ctl00$ctl00$PageBody$PageContent$DataPager1$ctl02$ctl00','')":
            current_page_urls = WebDriverWait(driver, 60).until(
                EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'more...')]")))
            for item in current_page_urls:
                urls_per_page = item.get_attribute("href")
                urls.append(urls_per_page)
            print("collected")
            sleep(2)
            try:
                next_page.click()
                print("next page clicked")
            except selenium.common.exceptions.StaleElementReferenceException as e:
                sleep(2)
                next_page = WebDriverWait(driver, 60).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), ' > ')]")))
                next_page.click()
                print("next page clicked")
            sleep(2)
        else:
            current_page_urls = driver.find_elements_by_xpath("//a[contains(text(), 'more...')]")
            for item in current_page_urls:
                urls_per_page = item.get_attribute("href")
                urls.append(urls_per_page)
            print("all collected for current type of bills")
            sleep(2)
            return urls


def bill_lists_prep_automation(driver, url, select_index):
    """
    automation clicks prior the beginning of scraping.
    param: driver
    param: url, string of bill url
    param: select_index an in representing the index of bill types
    """

    options_select_element = driver.find_element_by_id("ctl00_ctl00_PageBody_PageContent_ddlInstTypes2")
    options_select = Select(options_select_element)
    options_select.select_by_index(select_index)
    submit_btn = driver.find_element_by_id("ctl00_ctl00_PageBody_PageContent_btnSearchByInstRange")
    submit_btn.click()


def scrape(url):
    '''
    Insert logic here to scrape_rep all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.

    param: url, string of bill url
    '''

    row = scraper_utils.initialize_row()

    WEBDRIVER_PATH = "D:\work\IOTO\goverlytics-scrapers\web_drivers\chrome_win_90.0.4430.24\chromedriver.exe"

    driver = webdriver.Chrome(WEBDRIVER_PATH)

    driver.get(url)

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    row.source_url = url

    bill_name = get_bill_name(driver, row)

    session = get_session(driver, row)

    get_goverlytics_id(bill_name, row, session)

    chamber_origin = get_chamber_origin_and_bill_type(bill_name, row)

    bill_title = driver.find_element_by_id("ctl00_PageBody_LabelShortTitle").text
    row.bill_title = bill_title

    get_current_status(driver, row)

    get_date_introduced(driver, row, session)

    get_date_collected(row)

    get_principal_sponsor_and_id(driver, row, chamber_origin)

    get_sponsors_and_sponsors_id(driver, row, chamber_origin)

    get_bill_text(driver, row)

    get_action(driver, row, session)

    get_votes(chamber_origin, driver, row, bill_name)

    get_source_topic(bill_title, row)

    driver.quit()

    sleep(2)
    # Depending on the data you're able to collect, the legislation scraper may be more involved
    # Than the legislator scraper. For one, you will need to create the goverlytics_id. The
    # goverlytics_id is composed of the state, session, and bill_name, The goverlytics_id can be
    # created like so:
    # goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    # row.goverlytics_id = goverlytics_id

    # Once you have the goverlytics_id, you can create the url:
    # row.url = f'/us/{state_abbreviation}/legislation/{goverlytics_id}'

    # The sponsor and cosponsor ID's are where things can get complicated, depending on how
    # much and what kind of data the legislation page has on the (co)sponsors. The
    # legislator_id's are pulled from the legislator database table, so you must be able to
    # uniquely identify each (co)sponsor... using just a last name, for instance, is not
    # sufficient since often more than one legislator will have the same last name. If you
    # have a unique identifier such as the (co)sponsor's state_url or state_member_id, use
    # that. Otherwise, you will have to use some combination of the data available to
    # identify. Using a first and last name may be sufficient.

    # To get the ids, first get the identifying fields, then pass them into the
    # get_legislator_id() function:
    # row.principal_sponsor_id = scraper_utils.get_legislator_id(state_url=legislator_state_url)
    # The get_legislator_id function takes in any number of arguments, where the key is
    # the column in the legislator table you want to search, and the value is the value
    # you want to search that column for. So having:
    # name_first = 'Joe'
    # name_last = 'Jimbo'
    # row.principal_sponsor_id = get_legislator_id(name_first=name_first, name_last=name_last)
    # Will search the legislator table for the legislator with the first and last name Joe Jimbo.
    # Note that the value passed in must match exactly the value you are searching for, including
    # case and diacritics.

    # In the past, I've typically seen legislators with the same last name denoted with some sort
    # of identifier, typically either their first initial or party. Eg: A. Smith, or (R) Smith.
    # If this is the case, scraper_utils has a function that lets you search for a legislator
    # based on these identifiers. You can also pass in the name of the column you would like to
    # retrieve the results from, along with any additional search parameters:
    # fname_initial = 'A.'
    # name_last = 'Smith'
    # fname_initial = fname_initial.upper().replace('.', '') # Be sure to clean up the initial as necessary!
    # You can also search by multiple letters, say 'Ja' if you were searching for 'Jason'
    # goverlytics_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', fname_initial, name_last=name_last)
    # The above retrieves the goverlytics_id for the person with the first name initial "A" and
    # the last name "Smith".

    # Searching by party is similar:
    # party = '(R)'
    # name_last = 'Smith'
    # party = party[1] # Cleaning step; Grabs the 'R'
    # goverlytics_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'party', party, name_last=name_last)

    # Other than that, you can replace this statement with the rest of your scraper logic.

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    # pprint(row)

    return row


def get_current_status(driver, row):
    """
    current_status is located in a nobr tag in a span with the id "ctl00_PageBody_LabelCurrentStatus"
    """
    current_status = driver.find_element_by_id("ctl00_PageBody_LabelCurrentStatus").find_element_by_tag_name(
        "nobr").text
    row.current_status = current_status


def get_source_topic(bill_title, row):
    """
    source topic are located in the bill_title, left of the first colon.
    """
    source_topic = re.match("^.*?(?=:)", bill_title).group(0)
    row.source_topic = source_topic


def get_bill_name(driver, row):
    """
    bill_name is located in a span with the id "ctl00_PageBody_LabelBillID".
    """
    bill_name = driver.find_element_by_id("ctl00_PageBody_LabelBillID").text
    row.bill_name = bill_name
    return bill_name


def get_session(driver, row):
    """
    the session year is located in a span with the id "ctl00_PageBody_LabelSession".
    """
    full_session_name = driver.find_element_by_id("ctl00_PageBody_LabelSession").text
    session = full_session_name.replace(" REGULAR SESSION", "")
    row.session = session
    return session


def get_goverlytics_id(bill_name, row, session):
    """
    goverlytics_id need to take param of session and bill_name.
    """
    goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id


def get_chamber_origin_and_bill_type(bill_name, row):
    """
    chamber_origin and bill_type is determined by the id tag in bill_name.
    """
    if "HB" in bill_name:
        chamber_origin = 'House'
        bill_type = 'Bill'
    elif "HCR" in bill_name:
        chamber_origin = 'House'
        bill_type = 'Concurrent Resolution'
    elif "HCSR" in bill_name:
        chamber_origin = 'House'
        bill_type = 'Concurrent Study Request'
    elif "HR" in bill_name:
        chamber_origin = 'House'
        bill_type = 'Resolution'
    elif "HSR" in bill_name:
        chamber_origin = 'House'
        bill_type = 'Study Request'
    elif "SB" in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Bill'
    elif "SCR" in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Concurrent Resolution'
    elif "SR" in bill_name:
        chamber_origin = 'Senate'
        bill_type = 'Resolution'
    row.chamber_origin = chamber_origin
    row.bill_type = bill_type
    return chamber_origin


def get_date_introduced(driver, row, session):
    """
    date_introduced is the earliest date in actions.
    """
    date_introduced_short = driver.find_element_by_xpath(
        '//*[@id="ctl00_PageBody_PanelBillInfo"]/table[3]/tbody/tr[last()]/td[1]').text
    date_introduced = f"{date_introduced_short}/{session}"
    row.date_introduced = date_introduced


def get_date_collected(row):
    """
    date collected is the day that run the scraper.
    """
    date_collected = datetime.today
    row.date_collected = date_collected


def get_principal_sponsor_and_id(driver, row, chamber_origin):
    """
    principal sponsor is in the author span with the id "ctl00_PageBody_LinkAuthor". their name will be matched with
     their id in database with get_legislator_id.
    """
    principal_sponsor = driver.find_element_by_id("ctl00_PageBody_LinkAuthor").text
    for name in legislator_name_special_cases:
        if principal_sponsor == name:
            principal_sponsor = legislator_name_special_cases_reference[legislator_name_special_cases.index(name)]

    principal_sponsor_name_parser = HumanName(principal_sponsor)
    if chamber_origin == "House":
        principal_sponsor_id = scraper_utils.get_legislator_id(name_last=principal_sponsor_name_parser.last,
                                                               name_first=principal_sponsor_name_parser.first,
                                                               state_id=22)
    elif chamber_origin == "Senate":
        principal_sponsor_id = scraper_utils.get_legislator_id(name_last=principal_sponsor_name_parser.last,
                                                               name_first=principal_sponsor_name_parser.first,
                                                               state_id=22)
    else:
        principal_sponsor_id = scraper_utils.get_legislator_id(name_last=principal_sponsor_name_parser.last,
                                                               name_first=principal_sponsor_name_parser.first,
                                                               state_id=22)

    row.principal_sponsor = principal_sponsor
    row.principal_sponsor_id = principal_sponsor_id


def get_sponsors_and_sponsors_id(driver, row, chamber_origin):
    """
    sponsor is in the hidden dropdown authors table. their name will be matched with
     their id in database with get_legislator_id.
    """
    sponsors = []
    sponsors_id = []
    sponsors_elements = driver.find_elements_by_xpath(
        '//*[@id="ctl00_PageBody_PanelBillInfo"]/table[2]/tbody/tr/td/div[last()]/table/tbody/tr/td/table/tbody/tr/td/a')
    for sponsor in sponsors_elements:
        sponsor_name = sponsor.get_attribute("innerText")

        for name in legislator_name_special_cases:
            if sponsor_name == name:
                sponsor_name = legislator_name_special_cases_reference[legislator_name_special_cases.index(name)]

        if "(primary)" not in sponsor_name:
            sponsors.append(sponsor_name)
            sponsor_name_parser = HumanName(sponsor_name)
            if chamber_origin == "House":
                sponsor_id = scraper_utils.get_legislator_id(name_last=sponsor_name_parser.last,
                                                             name_first=sponsor_name_parser.first,
                                                             state_id=22)
            elif chamber_origin == "Senate":
                sponsor_id = scraper_utils.get_legislator_id(name_last=sponsor_name_parser.last,
                                                             name_first=sponsor_name_parser.first,
                                                             state_id=22)
            else:
                sponsor_id = scraper_utils.get_legislator_id(name_last=sponsor_name_parser.last,
                                                             name_first=sponsor_name_parser.first,
                                                             state_id=22)
            sponsors_id.append(sponsor_id)
    row.sponsors = sponsors
    row.sponsors_id = sponsors_id


def get_bill_text(driver, row):
    """
    the most updated bill text is in the first row in the hidden dropdown text table. It is in pdf file so it will be
    converted to string by pdfplumber.
    """
    item_bar_text = []
    item_bar = driver.find_elements_by_xpath(
        '//*[@id="ctl00_PageBody_MenuDocuments"]/tbody/tr/td/table/tbody/tr/td[1]/a')
    for item in item_bar:
        item_bar_text.append(item.text)
    if "Text" in item_bar_text:
        bill_text = ""
        text_location = driver.find_element_by_xpath(
            '//*[@id="ctl00_PageBody_PanelBillInfo"]/table[2]/tbody/tr/td/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td/a')
        pdf_link = text_location.get_attribute("href")
        response = requests.get(pdf_link, stream=True)
        pdf = pdfplumber.open(io.BytesIO(response.content))
        for page in pdf.pages:
            text = page.extract_text()
            if type(text) is str:
                bill_text += text
        bill_text = bill_text.replace("\n", " ")
        row.bill_text = bill_text


def get_action(driver, row, session):
    """
    action data is collected from the action table of each bill.
    """
    actions = []
    count = 1
    number_of_actions = len(driver.find_elements_by_xpath('//*[@id="ctl00_PageBody_PanelBillInfo"]/table[3]/tbody/tr'))
    while count <= number_of_actions:
        try:
            date_short = driver.find_element_by_xpath(
                f'//*[@id="ctl00_PageBody_PanelBillInfo"]/table[3]/tbody/tr[{count}]/td[1]').text
            chamber_short = driver.find_element_by_xpath(
                f'//*[@id="ctl00_PageBody_PanelBillInfo"]/table[3]/tbody/tr[{count}]/td[2]').text
            action = driver.find_element_by_xpath(
                f'//*[@id="ctl00_PageBody_PanelBillInfo"]/table[3]/tbody/tr[{count}]/td[4]').text

            formatted_date_short = date_short.replace("/", "-")
            date = session + "-" + formatted_date_short
            if chamber_short == "S":
                chamber = "Senate"
            elif chamber_short == "H":
                chamber = "House"
            actions.append({'date': date, 'action_by': chamber, 'description': action})
            count += 1
        except Exception:
            pass
    row.actions = actions


def get_votes(chamber_origin, driver, row, bill_name):
    """
    get the vote datas from each vote results pdf, convert them into the correct format for database.
    """
    item_bar_text = []
    item_bar = driver.find_elements_by_xpath(
        '//*[@id="ctl00_PageBody_MenuDocuments"]/tbody/tr/td/table/tbody/tr/td[1]/a')
    for item in item_bar:
        item_bar_text.append(item.text)
    if "Votes" in item_bar_text:
        all_vote_results = []
        votes_index = item_bar_text.index('Votes')
        votes_text_locations = driver.find_elements_by_xpath(
            f'//*[@id="ctl00_PageBody_PanelBillInfo"]/table[2]/tbody/tr/td/div[{votes_index + 1}]/table/tbody/tr/td/table/tbody/tr/td/a')
        for a in votes_text_locations:
            votes = []
            votes_text = ""
            pdf_link = a.get_attribute("href")
            response = requests.get(pdf_link, stream=True)
            pdf = pdfplumber.open(io.BytesIO(response.content))
            for page in pdf.pages:
                text = page.extract_text()
                formatted_text = text.replace('\n', ' ')
                votes_text += formatted_text
            date_string = re.search('Date: (.*?) ', votes_text)
            unformatted_date = datetime.strptime(date_string.group(1), "%m/%d/%Y")
            date = unformatted_date.strftime("%Y-%m-%d")
            description = a.get_attribute("innerText")
            yea = re.search('Total --\s*(\d+)\s*NAYS', votes_text).group(1)
            nay = re.search('Total --\s*(\d+)\s*ABSENT', votes_text).group(1)
            absent = re.search('(\d+)(?!.*\d)', votes_text).group(0)
            if 'PASSAGE' in description:
                passed = '1'
            else:
                passed = '0'
            chamber = chamber_origin
            yeas_string = re.search('YEAS(.*?)Total', votes_text).group(1)
            votes += votes_reformat(votes, yeas_string, "yea", chamber, bill_name, description)
            nays_string = re.search('NAYS(.*?)Total', votes_text).group(1)
            votes += votes_reformat(votes, nays_string, "nay", chamber, bill_name, description)
            absent_string = re.search('ABSENT(.*?)Total', votes_text).group(1)
            votes += votes_reformat(votes, absent_string, "absent", chamber, bill_name, description)
            all_vote_results.append({'date': date,
                                     'description': description,
                                     'yea': yea,
                                     'nay': nay,
                                     'absent': absent,
                                     'passed': passed,
                                     'chamber': chamber,
                                     'votes': votes
                                     })

        row.votes = all_vote_results


def votes_reformat(votes, name_string, votetext, chamber, bill_name, description):
    """
    reformat the voters string, their chamber and choice of vote into a list of dict, and then append in to the votes
     list. voter's name will be match with their id in database.
    """
    name_string = replace_speaker_and_president_name(name_string)
    name_string = name_string.replace(", ", ",")
    name_string = name_string.replace("St. ", "St.")
    name_string = name_string.split(" ")
    for name in name_string:
        if name != "":

            for n in legislator_name_special_cases:
                if name == n:
                    name = legislator_name_special_cases_reference[legislator_name_special_cases.index(n)]

            if "," in name:
                name = name.replace(".", "")
                name = name.replace(",", ", ")
                voter_full_name = name.split(", ")
                voter_id = scraper_utils.legislators_search_startswith(column_val_to_return="goverlytics_id",
                                                                       column_to_search="name_first",
                                                                       startswith=voter_full_name[1],
                                                                       name_last=voter_full_name[0],
                                                                       state_id=22)
                # try:
                #     voter_id = scraper_utils.legislators_search_startswith(column_val_to_return="goverlytics_id",
                #                                                             column_to_search="name_first",
                #                                                             startswith=voter_full_name[1],
                #                                                             name_last=voter_full_name[0],
                #                                                             state_id=22)
                # except Exception:
                #     voter_id = scraper_utils.legislators_search_startswith(column_val_to_return="goverlytics_id",
                #                                                             column_to_search="name_first",
                #                                                             startswith=voter_full_name[1],
                #                                                             name_last=voter_full_name[0],
                #                                                             state_id=22)
                votes.append({"goverlytics_id": voter_id, "legislator": name, "votetext": votetext})
                print(bill_name)
            elif "," not in name and "St." not in name:
                if "House" in description:
                    try:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Representative")
                    except Exception:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                elif "Senate" in description:
                    voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                else:
                    try:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Representative")
                    except Exception:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                votes.append({"goverlytics_id": voter_id, "legislator": name, "votetext": votetext})

            elif "," not in name and "St." in name:
                name = name.replace("St.", "St. ")
                if "House" in description:
                    try:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Representative")
                    except Exception:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                elif "Senate" in description:
                    voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                else:
                    try:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Representative")
                    except Exception:
                        voter_id = scraper_utils.get_legislator_id(name_last=name, state_id=22, role="Senator")
                votes.append({"goverlytics_id": voter_id, "legislator": name, "votetext": votetext})


    return votes


def replace_speaker_and_president_name(voters):
    """
    replace mr speaker and mr president in voter's name string with the name of the current Speaker of the House.
    """
    if "Mr. Speaker" in voters:
        page = requests.get("https://house.louisiana.gov/")
        soup = BeautifulSoup(page.content, 'html.parser')
        speaker = soup.find('span', {'id': 'h1ASpeaker'}).text
        speaker_last_name = HumanName(speaker.replace("Speaker of The House", "")).last
        voters = voters.replace("Mr. Speaker", speaker_last_name)

    if "Mr. President" in voters:
        page = requests.get("https://senate.la.gov/")
        soup = BeautifulSoup(page.content, 'html.parser')
        president = soup.find('span', {'class': 'fa-2x'}).text
        president_last_name = HumanName(president.replace("Senate President", "")).last
        voters = voters.replace("Mr. President", president_last_name)
    return voters


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape_rep:
    urls = get_urls()

    # Next, we'll scrape_rep the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape_rep(url) for url in urls]
    with Pool(7) as pool:
        data = pool.map(scrape, urls[:100])
    # for url in urls:
    #     data = scrape(url)
    #
    # # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)
    # scrape("https://legis.la.gov/legis/BillInfo.aspx?i=240255")

    print('Complete!')
