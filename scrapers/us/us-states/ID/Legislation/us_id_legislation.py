'''
Notes:
- Website layout changes in 2009, 2009 is first year with new layout
- actions with no specified date will have the date set to 9999/12/10
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary module
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
from datetime import datetime
import pdfplumber
import traceback
import io



state_abbreviation = 'ID'
database_table_name = 'us_id_legislation'
legislator_table_name = 'us_id_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

base_url = 'https://legislature.idaho.gov'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls(historical = False):
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    scrape_url = 'https://legislature.idaho.gov/sessioninfo/2021/legislation/'


    page = scraper_utils.request(scrape_url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    options = soup.find('select', id='ddlsessions').find_all('option')

    years = [[option['value'], option.text] for option in options]

    if not historical:
        years = [years[0]]

    bill_urls = []
    for year in years:
        page = scraper_utils.request(year[0])
        scraper_utils.crawl_delay(crawl_delay)
        soup = BeautifulSoup(page.content, 'html.parser')
        current_page = soup.find('div', id='hcode-tab-style2legislation-by-number')
        links = current_page.find_all('table')[2:]

        
        for link in links:
            try:
                bill_urls.append((link.find('a')['href'], year[0][41:-12]))
            except :
                pass

    return bill_urls

def to_votes(votes, voted, total):

    votes = votes[3:].split(',')

    if votes[0] == 'None':
        return []
    for v in votes:
        v = v.replace('\xa0', ' ')
        total.append({
            'goverlytics_id': scraper_utils.get_legislator_id(**{'name_last': v}),
            'legislator': v,
            'votet': voted
        })

    

def votes_helper(date, header, text, chamber):
    row_votes = []
    passed, vote_count = header[0].replace('\xa0', ' ').split('-', 1)
    vote_count = vote_count.split('-')
    try:
        int(vote_count[0])
    except:
        temp = header[0].replace('\xa0', ' ').split('-', 2)
        passed = temp[1]
        vote_count = temp[2]
        vote_count = vote_count.split('-')
    
    to_votes(text[2], 'yea', row_votes)
    to_votes(text[3], 'nay', row_votes)
    to_votes(text[4], 'absent', row_votes)
    

    ret_val = {'date': date,
    'description': text[0].replace('\xa0', ' '),
    'yea': vote_count[0], 'nay': vote_count[1], 'nv': 0, 'absent': vote_count[2], 'total': int(vote_count[0])  + int(vote_count[1]) + int(vote_count[2]),
    'passed': (int) (not 'FAILED' in passed),
    'chamber': chamber, 
    'votes': row_votes
    }


    return ret_val

def get_actions(bill_tables, row, year, url, chamber_origin):
    
    next_year = False
    last_date = datetime.strptime('1000/05/01', '%Y/%m/%d').date()
    actions = bill_tables[2].find_all('tr')
    actions = [t.find_all('td') for t in actions]
    row_actions = []
    current_chamber = chamber_origin
    introduced = False

    for action in actions:

            
        
        date = str(year) + '/' + action[1].text.strip()
        description = action[2]   
        
        has_vote = False
        if 'AYES' in description.text:
            has_vote = True
            span = description.find_all('span')
            span = [s.text for s in span]
            
            for s in description.find_all('span'):
                s.replaceWith(';;')
            plain_text = description.text.split(';;')
            
            
            row.votes.append(votes_helper(str(date), span, plain_text, current_chamber))

        try:
            try:
                date = datetime.strptime(date, '%Y/%m/%d').date()
            except:
                date = datetime.strptime('9999/12/10', '%Y/%m/%d').date()
            if not next_year and date < last_date:
                next_year = True
                year += 1
                date = datetime.strptime(date, '%Y/%m/%d').date()
        except ValueError:
            date = datetime.strptime('9999/12/10', '%Y/%m/%d').date()

        if not introduced:
            row.date_introduced = str(date)

        
        if has_vote:
            description = description.text[description.text.find(';;') + 2 : description.text.rfind(';;')].replace('\xa0', ' ')
        else:
            description = description.text.replace('\xa0', ' ')

        row_actions.append({'date': str(date), 'action_by': current_chamber, 'description': description})
        if 'to Senate' in description:
            current_chamber = 'Senate'
        elif 'to House' in description:
            current_chamber = 'House'

    row.actions = row_actions


def scrape(url):
    code = url[1]
    url = url[0]
    try:

        page = scraper_utils.request(base_url + url)
        scraper_utils.crawl_delay(crawl_delay)
        soup = BeautifulSoup(page.content, 'html.parser')
        bill_tables = soup.find_all('table', class_='bill-table')
        year = soup.find('h6').text[:4]

        row = scraper_utils.initialize_row()
        
        row.source_url = base_url + url

        td = bill_tables[0].find_all('td')
        bill_name = td[0].text.strip().replace('aaS', '')
        row.bill_name = bill_name
        row.goverlytics_id = state_abbreviation + '_' +  code + '_' + bill_name
        committee = td[-1].text.replace('  by ', '').title()
        if 'Committee' not in committee:
            row.principal_sponsor = committee
            id = scraper_utils.get_legislator_id(**{'name_last': committee})
            if id is not None:
                row.principal_sponsor_id = id
        row.committees.append(committee)
        row.principal_sponsor = committee.title()
        row.session = code
        row.source_id = code + '_' + bill_name

        if 'R' in bill_name:
            bill_type = 'Resolution'
            
        elif 'M' in bill_name:
            bill_type = 'Memorial'
        else: 
            bill_type = 'Bill'
            
        if 'H' in bill_name:
            chamber_origin = 'House'
        else:
            chamber_origin = 'Senate'

        row.bill_type = bill_type
        row.chamber_origin = chamber_origin



        try:
            div = soup.find('div', class_='wpb_column vc_column_container rounded col-xs-mobile-fullwidth col-sm-12')
            pdf_link = base_url + div.find('div', class_='vc-column-innner-wrapper').find('div').find('a')['href']
            response = requests.get(
                pdf_link, stream=True, headers=scraper_utils._request_headers)
            scraper_utils.crawl_delay(crawl_delay)
            pdf = pdfplumber.open(io.BytesIO(response.content))
            page = pdf.pages[0]
            row.bill_text = page.extract_text()
        except:
            traceback.print_exc()
            print(base_url + url)
            print(pdf_link)

        desc = bill_tables[1].find('td').text

        row.source_topic, row.bill_description = desc.split('–', 1)
        row.source_topic = row.source_topic.title()

        get_actions(bill_tables, row, year, url, chamber_origin)

        return row
    except:
        traceback.print_exc()
        print(base_url + url)

    


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)


    try:
        f = open("output.txt", "a")
        print(data, file=f)
        f.close()
    except:
        print('not saved')



    scraper_utils.write_data(data)

    print('Complete!')


