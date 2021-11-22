from platform import win32_is_iot
import sys
import os
from pathlib import Path

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from scraper_utils import CAProvTerrLegislatorScraperUtils
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import requests
from multiprocessing import Pool

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from nameparser import HumanName
import pandas as pd
from unidecode import unidecode 
import numpy as np
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


scraper_utils = CAProvTerrLegislatorScraperUtils('BC', 'ca_bc_legislators')
crawl_delay = scraper_utils.get_crawl_delay('https://www.leg.bc.ca')

#initialize selenium web driver
chrome_options = Options()
chrome_options.headless = True

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

print("driver found")

scraper_utils = CAProvTerrLegislatorScraperUtils('BC', 'ca_bc_legislators')


def get_urls(myurl):
    # get links of all the legislators' individual bio pages
    driver.get(myurl)
    timeout = 5

    try:
        element_present = EC.presence_of_element_located(
            (By.CLASS_NAME, 'BCLASS-Members-List'))
        WebDriverWait(driver, timeout).until(element_present)

    except:
        print("timeout")
        pass

    html = driver.page_source
    page_soup = soup(html, 'html.parser')
    # print(page_soup)
    urls = []

    # print(page_soup)
    member_list = page_soup.find("ul", {"class": "BCLASS-Members-List"})
    members = member_list.findAll("li")
    for mem in members:
        try:
            mem_link = mem.a['href']
            urls.append(mem_link)
        except:
            # print(mem)
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def scrape(url):
    row = scraper_utils.initialize_row()
    row.source_url = url
    # scrape legislators' bio pages
    url_broken = url.split("/")
    row.most_recent_term_id = url_broken[len(url_broken) - 2]

    driver.get(url)

    html = driver.page_source

    page_soup = soup(html, 'html.parser')

    ministertitle = page_soup.find("div", {"class": "col-xs-12 col-sm-12"})

    mlist = ministertitle.text.split('\n')

    name_container = page_soup.find('h2', {'class':'BCLASS-pagetitle'})
    row.name_full = name_container.text.replace('MLA: ', '').split(',')[0].strip()
    hn = HumanName(row.name_full)
    row.name_last = hn.last
    row.name_first = hn.first
    row.name_suffix = hn.suffix
    row.name_middle = hn.middle
    row.party = ""
    row.years_active = []
    row.riding = ""
    for item in mlist:
        try:
            if item.strip() != "":
                if "20" in item:
                    years = item.split(",")
                    for year in years:
                        if year.strip() not in row.years_active:
                            try:
                                year = int(year.strip())
                                row.years_active.append(year)
                            except:
                                pass
                elif "BC" in item:
                    row.party = (item.strip())
                elif "Minister" not in item and "Elected" not in item and "Speaker" not in item and \
                        "Deputy" not in item and "Council" not in item:
                    row.riding = (item.strip())

        except:
            pass

    row.party = row.party.split("BC ")[1]
    row.party = row.party.split(" Party")[0]
    if row.party == "NDP":
        row.party = "New Democratic"

    years_to_add = []
    for year in row.years_active:
        for i in range(4):
            if (year + i) < 2022:
                years_to_add.append(year + i)
    for y in years_to_add:
        if y not in row.years_active:
            row.years_active.append(y)
    row.years_active.sort()

    try:
        row.party_id = scraper_utils.get_party_id(row.party)

    except:
        row.party_id = 0

    email_class = page_soup.findAll("div", {"class": "convertToEmail"})

    email = email_class[1].text
    row.email = email

    row.addresses = []
    office_info = page_soup.find(
        "div", {"class": "BCLASS-Member-Info BCLASS-Hide-For-Vacant"})
    office = " ".join(office_info.text.split())
    addr_info = {'location': office.split(
        ":")[0].strip(), 'address': office.split(":")[1].strip()}
    row.addresses.append(addr_info)

    member_info = page_soup.findAll("div", {"class": "BCLASS-Constituency"})

    constituency_one = member_info[0].text
    constituency_two = member_info[0].nextSibling
    constituency_two = constituency_two.nextSibling
    constituency_two = constituency_two.nextSibling

    address = " ".join([constituency_one.strip(), constituency_two.strip()])
    address = " ".join(address.split())
    addr_info = {'location': 'Constituency', 'address': address}
    row.addresses.append(addr_info)

    row.committees = []
    committee_info = page_soup.find("div", {"class": "BCLASS-member-cmts"})
    if committee_info is None:
        committee_info = page_soup.find(
            "ul", {"class": "BCLASS-Members-Cmt-List"})
    try:
        coms = committee_info.findAll("li")
        for com in coms:
            committee_name = com.a.text

            committee = {'role': 'Member', 'committee': committee_name}
            row.committees.append(committee)
    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)

    row.phone_numbers = []
    contact = page_soup.find(
        "div", {"class": "BCLASS-Member-Info BCLASS-Contact"})
    contacts = contact.text.split('\n')
    opn = contacts[2].replace("(", "").strip()
    opn = opn.replace(") ", "-")
    office_phone = {'location': 'office phone', 'number': opn}
    row.phone_numbers.append(office_phone)
    if contacts[6] != "":
        ofn = contacts[6].replace("(", "").strip()
        ofn = ofn.replace(") ", "-")
        office_fax = {'location': 'office fax', 'number': ofn}
        row.phone_numbers.append(office_fax)

    const_info = page_soup.findAll("div", {"class": "col-xs-12 col-sm-6"})
    const_info = const_info[1].text
    const_info = const_info.split("\n")
    const_info = const_info[14:]
    cp_num = (const_info[2].strip())
    if "Fax" not in cp_num:
        cp_num = cp_num.replace("(", "")
        cp_num = cp_num.replace(") ", "-")
        cp = {'location': 'constituency phone', 'number': cp_num}
        row.phone_numbers.append(cp)

    c_fax = (const_info[6]).strip()
    if c_fax != "":
        c_fax = c_fax.replace("(", "")
        c_fax = c_fax.replace(") ", "-")
        cf = {'location': 'constituency fax', 'number': c_fax}

        row.phone_numbers.append(cf)

    ctf_num = (const_info[10]).strip()
    if ctf_num != "" and "Constituency" not in ctf_num:
        ctf_num = ctf_num.replace("1 (", "1-")
        ctf_num = ctf_num.replace("(", "")
        ctf_num = ctf_num.replace(") ", "-")
        ctf = {'location': 'constituency toll free', 'number': ctf_num}
        # print(ctf)
        row.phone_numbers.append(ctf)

    member_bio = page_soup.find("div", {"class":'row BCLASS-memberbio'}).text
    row.gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last, member_bio)

    wiki_base_url = "https://en.wikipedia.org"
    uClient = uReq(wiki_base_url + '/wiki/Parliament_of_British_Columbia')
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "html.parser")
    parliaments_table = page_soup.findAll("table", {'class': "wikitable"})[0]
    last_row = parliaments_table.findAll("tr")[-1]
    link_to_most_recent_parliament = wiki_base_url + last_row.findAll("td")[0].a["href"]

    uClient = uReq(link_to_most_recent_parliament)
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "html.parser")
    table = page_soup.findAll("table", {'class': 'wikitable sortable'})[0]
    table = table.findAll("tr")[1:]


    for table_row in table:
        tds = table_row.findAll("td")
        name_td = tds[0]
        name = name_td.text
        district = tds[-1].text
        
        if unidecode(row.riding.lower()) == unidecode(district.strip().lower()) and unidecode(row.name_last.lower()) in unidecode(name.strip().lower()):
            row.wiki_url = wiki_base_url + name_td.a['href']
            break

    scraper_utils.crawl_delay(crawl_delay)
    return row


def get_wiki_people(link):
    wiki_base_url = "https://en.wikipedia.org"
    # get links to legislators' personal wikipedia pages
    uClient = uReq(link)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    parliaments_table = page_soup.findAll("table", {'class': "wikitable"})[0]
    last_row = parliaments_table.findAll("tr")[-1]
    link_to_most_recent_parliament = wiki_base_url + last_row.findAll("td")[0].a["href"]

    uClient = uReq(link_to_most_recent_parliament)
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "html.parser")
    table = page_soup.findAll("table", {'class': 'wikitable sortable'})[0]
    table = table.findAll("tr")[1:]

    people_links = []
    for row in table:
        tds = row.findAll("td")
        name_td = tds[0]
        link = wiki_base_url + name_td.a["href"]
        people_links.append(link)

    scraper_utils.crawl_delay(crawl_delay)
    return people_links

try:
    if __name__ == '__main__':
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        members_link = 'https://www.leg.bc.ca/learn-about-us/members'
        # First we'll get the URLs we wish to scrape:
        urls = get_urls(members_link)

        with Pool() as pool:
            data = pool.map(scrape, urls)
        big_df = pd.DataFrame(data)
        big_df = big_df.drop(columns = ["birthday", "occupation", "education"])


        # get missing data from wikipedia

        wiki_bc_parliaments_link = 'https://en.wikipedia.org/wiki/Parliament_of_British_Columbia'
        wiki_people = get_wiki_people(wiki_bc_parliaments_link)

        with Pool() as pool:
            wiki_data = pool.map(scraper_utils.scrape_wiki_bio, wiki_people)

        wiki_df = pd.DataFrame(wiki_data)[
            ['occupation', 'education', 'birthday', 'wiki_url']]
        # print(wiki_df)

        mergedRepsData = pd.merge(big_df, wiki_df, how='left', on=[
                                "wiki_url"])

        mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({np.nan: None})
        mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({np.nan: None})
        mergedRepsData['education'] = mergedRepsData['education'].replace({np.nan: None})

        big_df = mergedRepsData
        big_list_of_dicts = big_df.to_dict('records')
        # print(big_list_of_dicts)

        print('Writing data to database...')

        scraper_utils.write_data(big_list_of_dicts)

        print('Complete!')
except Exception as e:
    print(e)
    sys.exit(1)