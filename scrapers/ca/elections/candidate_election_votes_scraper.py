import sys
import os
from pathlib import Path
import time
from selenium import webdriver
import scraper_utils
from scraper_utils import CandidateElectionVotesScraperUtils
from database import CursorFromConnectionFromPool
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser as dparser

NODES_TO_ROOT = 4
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

PATH = "../../../web_drivers/chrome_win_93.0.4577.15/chromedriver.exe"
browser = webdriver.Chrome(PATH)

# https://www.elections.ca/content.aspx?section=ele&dir=pas&document=index&lang=e
COUNTRY = 'ca'
TABLE = 'ca_candidate_election_votes'
MAIN_URL = 'https://www.elections.ca/'
ELECTIONS_URL = MAIN_URL + 'content.aspx?section=ele&dir=pas&document=index&lang=e'
THREADS_FOR_POOL = 12

scraper_utils = CandidateElectionVotesScraperUtils(COUNTRY, TABLE)
crawl_delay = scraper_utils.get_crawl_delay(MAIN_URL)

with CursorFromConnectionFromPool() as cur:
    try:
        query = 'SELECT * FROM ca_elections'
        cur.execute(query)
        election = cur.fetchall()

        query = 'SELECT * FROM ca_candidates'
        cur.execute(query)
        candidates = cur.fetchall()

        query = 'SELECT * FROM ca_candidate_election_details'
        cur.execute(query)
        election_details = cur.fetchall()

    except Exception as e:
        sys.exit(
            f'An exception occurred retrieving tables from database:\n{e}')

    elections = pd.DataFrame(election)
    candidates_table = pd.DataFrame(candidates)
    election_details = pd.DataFrame(election_details)


def find_election_votes_links(link):
    page = scraper_utils.request(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    main = soup.find('div', {'id': 'content-main'})
    voters_links = main.find_all('a')
    for link in voters_links:
        if "Voting" in link.text:
            voter_link = link.get('href')
            voter_link = MAIN_URL + voter_link
            scraper_utils.crawl_delay(crawl_delay)
            return voter_link


def get_urls():
    urls = []
    election_links = []

    page = scraper_utils.request(ELECTIONS_URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    links_section = soup.find('div', {'id': 'content-main'})
    elections = links_section.find_all('a')
    for election in elections[2:]:
        link = election.get('href')
        link = MAIN_URL + link
        election_links.append(link)
    for link in election_links:
        link = find_election_votes_links(link)
        if link is not None:
            urls.append(link)
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def get_table_data(url):
    #print(url)
    data = None
    browser.get(url)
    if browser.find_elements_by_tag_name('frame'):

        frames = (browser.find_elements_by_tag_name('frame'))
        browser.switch_to.frame(frames[0])
        input_tag = browser.find_element_by_tag_name("input")
        try:
            input_value = int(input_tag.get_attribute('value'))
        except:
            option_tag = browser.find_element_by_tag_name("option")
            input_value = int(option_tag.get_attribute('value'))
        url = url.replace('default.html', '')
        url = url.replace('home.html', '')
        data = get_votes_with_frame(input_value, url)
    elements = browser.find_elements_by_tag_name('a')
    for e in elements:
        link = e.get_attribute('href')
        # opening the table tab
        try:
            if '#3' in link:
                e.click()
                break
            elif '#1' in link:
                e.click()
                break
        except:
            pass
    links = browser.find_elements_by_tag_name('a')
    time.sleep(5)
    for l in links:
        try:
            if 'candidates by electoral district and individual results' in l.text:
                l.click()
                data = get_candidate_data_by_district_table()

                break
            if 'List of candidates and individual results' in l.text:
                l.click()
                data = get_candidate_data()

                break
        except Exception as e:
            print(e)
    try:
        for item in data:
            gov_id = get_goverlytics_id(item['candidate_name'])
            election_id = get_election_id(item['election_name'])
            can_elec_id = get_candidate_election_details(gov_id, election_id)
            item.update({'candidate_election_id': can_elec_id})

    except:
        pass
    return data
        # if row_list is None:
        #     row_list = []
        # #print(row_list)
        # #print()
        # return row_list


def get_votes_with_frame(input_value, url):

    browser.get(url + f'{input_value}/table12.html')
    candidate_name_and_votes_list = []
    try:
        election_name = browser.find_element_by_tag_name('table')
        name = election_name.text.split('\n')[0]
    except:
        pass
    tables = browser.find_elements_by_tag_name('tbody')
    rows = tables[1].find_elements_by_tag_name('tr')
    for r in rows:
        try:
            #print(r.text)
            columns = r.find_elements_by_tag_name('td')
            candidate_name = columns[1].text.split('\n')[0].replace(' **', '')
            votes = columns[4].text.replace(' ', '')
            votes_percent = columns[5].text
            try:
                majority = columns[6].text.replace(' ', '')
                majority_percent = columns[7].text
            except:
                majority = 0
                majority_percent = 0
            candidate = {'election_name': name, 'candidate_name': candidate_name, 'votes': votes,
                         'votes_percent': votes_percent, 'majority': majority,
                         'majority_percent': majority_percent}
            candidate_name_and_votes_list.append(candidate)
        except:
            rows = tables[0].find_elements_by_tag_name('tr')
            for r in rows[7:]:
                try:
                    #print(r.text)
                    columns = r.find_elements_by_tag_name('td')
                    candidate_name = columns[1].text.split('\n')[0].strip().replace(' **', '')
                    votes = columns[4].text.replace(' ', '')
                    votes_percent = columns[5].text
                    try:
                        majority = columns[6].text.replace(' ', '')
                        majority_percent = columns[7].text
                    except:
                        majority = 0
                        majority_percent = 0
                    candidate = {'election_name': name, 'candidate_name': candidate_name, 'votes': votes,
                                 'votes_percent': votes_percent, 'majority': majority,
                                 'majority_percent': majority_percent}
                    candidate_name_and_votes_list.append(candidate)
                except:
                    pass

    return candidate_name_and_votes_list


def get_candidate_data():

    candidate_name_and_votes_list = []
    election_name = browser.find_element_by_tag_name('h1').text
    tables = browser.find_elements_by_tag_name('table')
    try:
        tbody = tables[10].find_element_by_tag_name('tbody')
        rows = tbody.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                # print(r.text)
                columns = r.find_elements_by_tag_name('td')
                candidate_name = columns[2].text.split('\n')
                votes = columns[5].text.split('\n')
                votes_percent = columns[6].text.split('\n')
                for i in range(0, len(candidate_name)):
                    candidate = {'election_name': election_name, 'candidate_name': candidate_name[i].replace(' **', ''), 'votes': votes[i],
                                 'votes_percent': votes_percent[i], 'majority': None, 'majority_percent': None}
                    candidate_name_and_votes_list.append(candidate)
            except:
                pass
    except:
        pass
    if len(candidate_name_and_votes_list) < 1:
        rows = []
        tables = browser.find_elements_by_tag_name('table')
        titles = browser.find_elements_by_tag_name('caption')
        count = 0
        for t in titles:
            if "List of candidates and individual results" in t.text:
                count += 1

        if count > 1:
            for i in range(0, count-1):
                rows.extend(tables[2+i].find_elements_by_tag_name('tr'))
        else:
            rows.append(tables[2].find_elements_by_tag_name('tr'))
        for r in rows:

            try:
                columns = r.find_elements_by_tag_name('td')
                try:
                    candidate_name = columns[1].text
                    try:
                        candidate_name = candidate_name.split('\n')[0]
                    except:
                        pass
                    votes = columns[4].text
                    votes_percent = columns[5].text
                except:
                    candidate_name = columns[0].text
                    try:
                        candidate_name = candidate_name.split('\n')[0]
                    except:
                        pass
                    votes = columns[3].text
                    votes_percent = columns[4].text
                candidate = {'election_name': election_name, 'candidate_name': candidate_name.replace(' **', ''), 'votes': votes,
                             'votes_percent': votes_percent, 'majority': None, 'majority_percent': None}
                candidate_name_and_votes_list.append(candidate)
            except Exception as e:
                pass
        if candidate_name_and_votes_list[0]['candidate_name'] == 'Number of electors and polling stations':
            candidate_name_and_votes_list = []
        if len(candidate_name_and_votes_list) < 1:
            tables = browser.find_elements_by_tag_name('table')
            for t in tables:
                try:
                    if "List of candidates and individual results" in t.text:
                        table = t
                    elif "Candidate and affiliation" in t.text:
                        table = t
                except Exception as e:
                    print(e)
            rows = table.find_elements_by_tag_name('tr')
            for r in rows[2:]:
                try:
                    columns = r.find_elements_by_tag_name('td')
                    candidate_name_list = columns[1].text
                    candidate_name_list = candidate_name_list.split('\n')
                    votes_list = columns[4].text
                    votes_list = votes_list.split('\n')
                    votes_percent_list = columns[5].text
                    votes_percent_list = votes_percent_list.split('\n')
                    for i in range(0, len(candidate_name_list)):
                        if i/2 == 0:
                            candidate_name = candidate_name_list[i].split(', ')[1] + ' ' + candidate_name_list[i].split(', ')[0]
                            votes = votes_list[i].replace(' ', '')
                            votes_percent = votes_percent_list[i]
                        candidate = {'election_name': election_name, 'candidate_name': candidate_name.replace(' **', ''), 'votes': votes,
                                 'votes_percent': votes_percent, 'majority': None, 'majority_percent': None}
                    candidate_name_and_votes_list.append(candidate)
                except:
                    pass

    return candidate_name_and_votes_list


def get_candidate_data_by_district_table():

    candidate_name_and_votes_list = []
    try:
        election_name = browser.find_element_by_tag_name('h1').text
        tbody = browser.find_element_by_tag_name('tbody')
        rows = tbody.find_elements_by_tag_name('tr')
        for r in rows:
            try:
                columns = r.find_elements_by_tag_name('td')
                candidate_name = columns[0].text.replace(' **', '')
                try:
                    candidate_name = candidate_name.split('\n')[0]
                except:
                    pass
                votes = columns[3].text.replace(',', '')
                votes_percent = columns[4].text
                try:
                    majority = columns[5].text.replace(',', '')
                    majority_percent = columns[6].text
                except:
                    majority = 0
                    majority_percent = 0
                candidate = {'election_name': election_name, 'candidate_name': candidate_name, 'votes': votes,
                             'votes_percent': votes_percent, 'majority': majority, 'majority_percent': majority_percent}
                candidate_name_and_votes_list.append(candidate)
            except:
                pass
    except:
         candidate_name_and_votes_list = get_data_by_province()

    return candidate_name_and_votes_list


def get_data_by_province():

    candidate_name_and_votes_list = []
    links = browser.find_elements_by_tag_name('a')
    for l in links:
        #print(l.text)
        if "12" in l.text:
            link = l.get_attribute('href')
            browser.get(link)
            election_name = browser.find_element_by_tag_name('h1').text
            table = browser.find_elements_by_tag_name('table')
            rows = table[1].find_elements_by_tag_name('tr')
            for r in rows[2:]:
                try:
                    columns = r.find_elements_by_tag_name('td')
                    candidate_name = columns[1].text.replace(' **', '')
                    votes = columns[4].text.replace(' ', '')
                    votes_percent = columns[5].text
                    try:
                        majority = columns[6].text.replace(' ', '')
                        majority_percent = columns[7].text
                    except:
                        majority = 0
                        majority_percent = 0
                    candidate = {'election_name': election_name, 'candidate_name': candidate_name, 'votes': votes,
                                 'votes_percent': votes_percent, 'majority': majority,
                                 'majority_percent': majority_percent}
                    candidate_name_and_votes_list.append(candidate)
                except:
                    pass
        return candidate_name_and_votes_list


def get_election_id(election):
    if 'by-election' in election.lower():
        try:
            date = dparser.parse(election, fuzzy=True)
            date_name = date.strftime("%Y_%m_%d")
            election_name = 'by_election_' + date_name
        except Exception as e:
            print(e)
        if pd.notna(election_name):
            df = elections
            value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
            try:
                return int(value)
            except Exception as e:
                return value
    if 'general' in election.lower():
        general_elections = {
            'fiftieth': '50',
            'forty-ninth': '49',
            'forty-eighth': '48',
            'forty-seventh': '47',
            'forty-sixth': '46',
            'forty-fifth': '45',
            'forty-fourth': '44',
            'forty-third': '43',
            'forty-second': '42',
            'forty-first': '41',
            'fortieth': '40',
            'thirty-ninth': '39',
            'thirty-eighth': '38',
            'thirty-seventh': '37',
            'thirty-sixth': '36'
        }
        election = election.split(' ')[0].lower()
        e_number = general_elections.get(election)
        election_name = e_number + '_general_election'
        if pd.notna(election_name):
            df = elections
            value = df.loc[df['election_name'].str.contains(election_name)]['id'].values[0]
            try:
                #print(value)
                return int(value)
            except Exception as e:
                return value


def get_goverlytics_id(name):

    last_name = name.split(' ')[-1]
    #print(last_name)
    first_name = name.split(' ')[0]
    #print(first_name)
    if pd.notna(name):
        df = candidates_table
        try:
            gov_id = df.loc[(df["name_first"].str.contains(first_name)) & (df["name_last"].str.contains(last_name))]['goverlytics_id'].values[0]
        except:
            gov_id = 0
    try:
        #print(gov_id)
        return int(gov_id)
    except Exception:
        return 0


def get_candidate_election_details(gov_id, election_id):
    if pd.notna(gov_id):
        df = election_details
        try:
            candidate_election_id = df.loc[(df["candidate_id"] == gov_id) & (df["election_id"] == election_id)]['id'].values[0]
        except:
            candidate_election_id = 0
    try:
        #print(candidate_election_id)
        return int(candidate_election_id)
    except Exception:
        return 0


def get_row_data(data):
    try:
        votes = data['votes'].replace(',', '').replace(' ', '')
    except:
        pass
    try:
        votes_percent = data['votes_percent'].replace(' ', '')
    except:
        pass
    try:
        majority = data['majority'].replace(',', '').replace(' ', '')
        if majority == '':
            majority = 0
        else:
            majority = data['majority']
    except:
        pass
    try:
        majority_percent = data['majority_percent'].replace(' ', '')
        if data['majority_percent'] == '':
            majority_percent = 0
        else:
            majority_percent = data['majority_percent']
    except:
        pass
    try:
        if data['candidate_election_id'] != 0:
            row = scraper_utils.initialize_row()
            row.candidate_election_id = int(data['candidate_election_id'])
            row.votes_obtained = int(votes)
            row.votes_percentage = float(votes_percent)
            row.majority = int(majority)
            row.majority_percentage = float(majority_percent)
            return row
    except Exception as e:
        print(e)


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')
    urls = get_urls()
    data = []
    data_list = []
    data.extend(get_table_data(url) for url in urls)

    #get_table_data('https://www.elections.ca//content.aspx?section=res&dir=rep/off/ovr_2012b&document=index&lang=e')
    lambda_obj = lambda x: (x is not None)

    list_out = list(filter(lambda_obj, data))

    flat_ls = [item for sublist in list_out for item in sublist]

    # with Pool(processes=4) as pool:
    #     data = pool.map(scrape, urls)
    # print(data_list)
    #
    row_data = [get_row_data(d) for d in flat_ls]
    res = [i for i in row_data if i]

    scraper_utils.write_data(res)

    print('Complete!')