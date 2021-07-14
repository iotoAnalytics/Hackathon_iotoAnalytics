import sys
import os
from pprint import pprint
from pathlib import Path
from nameparser import HumanName
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from multiprocessing import Pool
from time import sleep
from tika import parser
from scraper_utils import USStateLegislationScraperUtils

p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

BASE_URL_2021 = 'https://le.utah.gov/asp/billsintro/SubResults.asp?Listbox4=ALL'

state_abbreviation = 'VT'
database_table_name = 'us_ut_legislation'
legislator_table_name = 'us_ut_legislators'

scraper_utils = USStateLegislationScraperUtils(state_abbreviation, database_table_name, legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL_2021)


"""
I call open driver in each function which slows down scraping SIGNIFICANTLY. The alternative is writing everything in 
one function and using a single webdriver object which would increase scrape speed but at the cost of code readability. The 
other alternative is to pass a driver object from the scrape() function but because we are using pool(), we end up calling
too many driver requests at once which violates the site crawl delay (connection timeout).
"""


def open_driver(url):
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(executable_path=os.path.join('..', '..', '..', '..', '..', 'web_drivers',
                                                           'chrome_win_90.0.4430.24', 'chromedriver.exe'),
                              options=options)
    driver.get(url)
    driver.maximize_window()
    scraper_utils.crawl_delay(crawl_delay)
    return driver


def make_soup(url):
    """
    Takes URL and returns soup object.

    :param url: string representing url paths
    :return: soup object
    """

    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'lxml')
    scraper_utils.crawl_delay(crawl_delay)
    return soup


@scraper_utils.Timer()
def get_urls(url):
    urls = []
    soup = make_soup(url)
    form = soup.find('form', {'name': 'thisForm'})
    link_tables = form.find_all('div', {'class': 'subresults'})

    for div in link_tables[0:10]:
        links = div.find('table').find_all('tr')[1:]
        for link in links:
            url = link.find('a').get('href')
            if url not in urls:
                urls.append(url)
    return urls


def get_goverlytics_id(soup, row):
    bread = soup.find('ul', {'id': 'breadcrumb'})
    year = bread.find_all('li')[1].text.split()[0]
    bill_name = bread.find_all('li')[-1].text.replace('.', '').replace(' ', '')
    gov_id = f'ut_{year}_{bill_name}'
    row.goverlytics_id = gov_id


def get_bill_name(soup, row):
    bread = soup.find('ul', {'id': 'breadcrumb'})
    bill_name = bread.find_all('li')[-1].text.replace('.', '').replace(' ', '')
    row.bill_name = bill_name


def get_bill_text(url, row):
    driver = open_driver(url)
    content = driver.find_element_by_xpath('//*[@id="content"]')
    row.bill_text = content.text


def get_bill_sponsor(url, row):
    driver = open_driver(url)
    print(url)
    sponsors = []
    sponsors_ids = []
    cosponsors = []
    cosponsors_ids = []

    principal = driver.find_element_by_xpath('//*[@id="billsponsordiv"]/a').text.split()[1:]
    principal_first, principal_last = principal[1], principal[0].replace(',', '')
    principal_name = principal_first + ' ' + principal_last
    principal_id = scraper_utils.get_legislator_id(name_last=principal_last, name_first=principal_first)

    try:
        floor = driver.find_element_by_xpath('//*[@id="floorsponsordiv"]/a').text.split()[1:]
        floor_first, floor_last = floor[1], floor[0].replace(',', '')
        floor_id = scraper_utils.get_legislator_id(name_last=floor_last, name_first=floor_first)
        sponsors.append(floor_last)
        sponsors_ids.append(floor_id)
    except IndexError:
        pass

    try:
        substitute = driver.find_element_by_xpath('//*[@id="legislatordiv"]/ul[1]/li/a').text.split()[1:]
        substitute_first, substitute_last = substitute[1], substitute[0].replace(',', '')
        substitute_id = scraper_utils.get_legislator_id(name_last=substitute_last, name_first=substitute_first)
        sponsors.append(substitute_last)
        sponsors_ids.append(substitute_id)
    except NoSuchElementException:
        pass

    try:
        cosponsors_table = driver.find_element_by_xpath('//*[@id="legislatordiv"]/ul[2]/li/ul/table').find_elements_by_tag_name('tr')
        for tr in cosponsors_table:
            rows = tr.find_elements_by_tag_name('td')
            for td in rows:
                cosponsor_name = td.text.split()
                cosponsor_first = cosponsor_name[1]
                cosponsor_last = cosponsor_name[0].replace(',', '')
                cosponsor_id = scraper_utils.legislators_search_startswith(column_val_to_return='goverlytics_id',
                                                                           column_to_search='name_first',
                                                                           startswith=cosponsor_first[0],
                                                                           name_last=cosponsor_last)
                print(cosponsor_last)
                cosponsors.append(cosponsor_last)
                cosponsors_ids.append(cosponsor_id)
    except (NoSuchElementException, AttributeError):
        pass

    row.principal_sponsor = principal_name
    row.principal_sponsor_id = principal_id
    row.cosponsors = cosponsors
    row.cosponsors_id = cosponsors_ids
    row.sponsors = sponsors
    row.sponsors_id = sponsors_ids
    driver.quit()


def get_bill_session(soup, row):
    breadcrumb = soup.find('ul', {'id': 'breadcrumb'})
    session = breadcrumb.find_all('li')[1].text.split()[0]
    row.session = session


def get_white_vote(url, row):
    votes = []
    vote_dict = {}
    driver = open_driver(url)
    totals_row = driver.find_element_by_xpath('//*[@id="main-content"]/div/article/font[2]/center/table/tbody/tr[2]').find_elements_by_tag_name('td')
    yea, nay, absent = totals_row[0].text, totals_row[2].text, totals_row[-1].text
    yea, nay, absent = yea.split('-')[-1].strip(), nay.split('-')[-1].strip(), absent.split('-')[-1].strip()

    chamber = driver.find_element_by_xpath('//*[@id="main-content"]/div/article/center/font[1]/b').text.split()[0]
    description = driver.find_element_by_xpath('//*[@id="main-content"]/div/article/center/font[2]/b').text
    date = driver.find_element_by_xpath('/html/body/main/div/article/center/font[2]/text()[1]').split()[0].split('/')
    date = date[-1] + '-' + date[0] + '-' + date[1]

    table = driver.find_element_by_xpath('//*[@id="main-content"]/div/article/font[2]/center/table').find_elements_by_tag_name('tr')[-1]
    columns = table.find_elements_by_tag_name('td')

    vote_info = []
    yea_voters, nay_voters, absent_voters = columns[0].text.split('\n'), columns[2].text.split('\n'), columns[-1].text.split('\n')
    for voter in yea_voters:
        if voter == '':
            pass
        else:
            gov_id = scraper_utils.legislators_search_startswith(column_val_to_return='goverlytics_id', column_to_search='name_first',
                                                             startswith=voter.split(',')[-1].strip()[0], name_last=voter.split(',')[0])
            vote_info.append({'vote': 'Yea', 'legislator': voter, 'goverlyics_id': gov_id})
    for voter in nay_voters:
        if voter == '':
            pass
        else:
            gov_id = scraper_utils.legislators_search_startswith(column_val_to_return='goverlytics_id', column_to_search='name_first',
                                                             startswith=voter.split(',')[-1].strip()[0], name_last=voter.split(',')[0])
            vote_info.append({'vote': 'Nay', 'legislator': voter, 'goverlyics_id': gov_id})
    for voter in absent_voters:
        if voter == '':
            pass
        else:
            gov_id = scraper_utils.legislators_search_startswith(column_val_to_return='goverlytics_id', column_to_search='name_first',
                                                             startswith=voter.split(',')[-1].strip()[0], name_last=voter.split(',')[0])
            vote_info.append({'vote': 'Absent', 'legislator': voter, 'goverlyics_id': gov_id})

    vote_dict['yea'], vote_dict['nay'], vote_dict['absent'] = yea, nay, absent
    vote_dict['votes'], vote_dict['chamber'], vote_dict['description'], vote_dict = vote_info, chamber, description, date
    votes.append(vote_dict)
    row.votes = votes
    pprint(row.votes, url)


def get_blue_red_vote(url, row):
    pass


def get_bill_actions_and_votes(url, row):
    actions = []
    driver = open_driver(url)
    button = driver.find_element_by_xpath('//*[@id="activator-billStatus"]')
    button.click()
    table = driver.find_element_by_xpath('//*[@id="billStatus"]/div/table').find_elements_by_tag_name('tr')
    date_introduced = table[2].find_elements_by_tag_name('td')
    date_introduced = date_introduced[0].text.split()[0].split('/')
    date_introduced = date_introduced[2] + '-' + date_introduced[0] + '-' + date_introduced[1]
    for tr in table[1:]:
        table_row = tr.find_elements_by_tag_name('td')
        date_info = table_row[0].text.split()[0].split('/')
        date = date_info[2] + '-' + date_info[0] + '-' + date_info[1]
        action = table_row[1].text
        action_by = ''
        font_color = table_row[0].find_element_by_tag_name('font').value_of_css_property('color')
        if font_color == 'rgba(204, 0, 0, 1)':
            action_by = 'House'
            try:
                vote_link = table_row[-1].find_element_by_tag_name('a').get_attribute('href')
                get_blue_red_vote(vote_link, row)
            except NoSuchElementException:
                pass
        elif font_color == 'rgba(51, 51, 255, 1)':
            action_by = 'Senate'
            try:
                vote_link = table_row[-1].find_element_by_tag_name('a').get_attribute('href')
                get_blue_red_vote(vote_link, row)
            except NoSuchElementException:
                pass
        else:
            try:
                vote_link = table_row[-1].find_element_by_tag_name('a').get_attribute('href')
                get_white_vote(vote_link, row)
            except NoSuchElementException:
                pass
        actions.append({'date': date, 'action_by': action_by, 'description': action})
    row.actions = actions
    row.date_introduced = date_introduced
    driver.quit()


def get_bill_type(url, row):
    info = url.split('/')
    bill_name = info[-1].split('.')[0]
    bill_letters = list(bill_name)
    if 'R' and 'C' in bill_letters:
        row.bill_type = 'Concurrent Resolution'
    elif 'R' and 'J' in bill_letters:
        row.bill_type = 'Joint Resolution'
    elif 'R' in bill_letters:
        row.bill_type = 'Resolution'
    else:
        row.bill_type = 'Bill'


def get_bill_name(url, row):
    driver = open_driver(url)
    name = driver.find_element_by_xpath('//*[@id="substrdiv"]/h2').text
    bill_name = name.replace('.', '').replace(' ', '')
    row.bill_name = bill_name
    driver.quit()


def get_bill_title(url, row):
    driver = open_driver(url)
    name = driver.find_element_by_xpath('//*[@id="main-content"]/div/article/h3').text
    bill_title = " ".join(name.split()[2:])
    row.bill_title = bill_title
    driver.quit()


def get_current_status(url, row):
    driver = open_driver(url)
    info = driver.find_element_by_xpath('//*[@id="billinfo"]').find_elements_by_class_name('billinfoulm')
    for ul in info:
        heading = ul.find_element_by_tag_name('li').find_element_by_tag_name('b').text
        if heading == 'Information':
            last_action = ul.find_element_by_tag_name('li').find_element_by_tag_name('ul').find_element_by_tag_name('li').text
            current_status = last_action.split(': ')[-1]
            row.current_status = current_status
    driver.quit()


def get_bill_committees(url, row):
    committees = []
    driver = open_driver(url)
    button = driver.find_element_by_xpath('//*[@id="activator-billVideo"]')
    button.click()
    print(url)
    try:
        table = driver.find_element_by_xpath('//*[@id="billVideo"]/ul[1]/li/ul').find_elements_by_tag_name('li')
        print(len(table))
        for li in table:
            x = li.find_element_by_tag_name('a')

        #     committee_name = li.find_elements_by_tag_name('a')[0].text
            print(x.text)
            print()

            # chamber = committee_name.split()[0]
            # committees.append({'chamber': chamber, 'committee_name': committee_name})
    except NoSuchElementException:
        pass
    print('__________________________________________________')
    row.committees = committees
    # print(url, row.committees)


@scraper_utils.Timer()
def scrape(url):
    row = scraper_utils.initialize_row()
    soup = make_soup(url)
    # get_goverlytics_id(soup, row)
    # get_bill_text(url, row)
    # get_bill_sponsor(url, row)
    # get_bill_session(soup, row)
    # get_bill_actions_and_votes(url, row)
    # get_bill_name(url, row)
    # get_bill_type(url, row)
    # get_bill_title(url, row)
    # get_bill_committees(url, row) ### not working
    # get_current_status(url, row)
    # print(row)
    return row


def main():
    urls = get_urls(BASE_URL_2021)

    with Pool() as pool:
        data = list(tqdm(pool.imap(scrape, urls)))


if __name__ == '__main__':
    main()
