'''
Author: Avery Quan
Date: May 11 2021

Notes:

'''
import sys
import os
from pathlib import Path

from numpy import get_printoptions

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import re
import math
import pandas as pd
import pdfplumber
import traceback
import io
from tqdm import tqdm
import numpy as np

state_abbreviation = 'CO'
database_table_name = 'us_co_legislation'
legislator_table_name = 'us_co_legislators'
scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)
base_url = 'https://leg.colorado.gov'
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls(historical = True):
    data = []

    session_codes = []
    bill_search = 'https://leg.colorado.gov/bill-search?field_chamber=All&field_bill_type=All&field_sessions=66816&sort_bef_combine=field_bill_number%20ASC'
    page = scraper_utils.request(bill_search)
    soup = BeautifulSoup(page.content, 'html.parser')
    sessions = soup.find('select', id='edit-field-sessions').find_all('option')[1:]
    for session in sessions:
        session_codes.append((session['value'], session.text))

    if not historical:
        session_codes = [session_codes[0]]

    #   SCRAPES ONE SESSION  AT A TIME REMOVE IT LATER
    session_codes = [session_codes[7]]

    for code in session_codes:
        bill_url = f'https://leg.colorado.gov/bill-search?field_chamber=All&field_bill_type=All&field_sessions={code[0]}&sort_bef_combine=field_bill_number%20ASC'
        page = scraper_utils.request(bill_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        pages = soup.find('div', class_= 'view-header').text
        # gets num of pages 
        pages = math.ceil(int(re.findall(r'\d+', pages)[-1])/25)
        scraper_utils.crawl_delay(crawl_delay)
    
    
        for i in tqdm(range(pages)):
            try:
                page_url = bill_url + '&page=' + str(i)

                # page_url = 'https://leg.colorado.gov/bill-search?field_sessions=66816&sort_bef_co`mbine=field_bill_number%20ASC'
                page = scraper_utils.request(page_url)
                scraper_utils.crawl_delay(crawl_delay)
                soup = BeautifulSoup(page.content, 'html.parser')
                bills = soup.find_all('article', )
                for bill in bills:
                    title = bill.find('h4')
                    url = base_url + title.find('a')['href']
                    desc = bill.find('div', class_='field field-name-field-bill-long-title field-type-text-long field-label-hidden').text.strip()
                    last_action = bill.find('div', class_='bill-last-action search-result-single-item').find('span').text.split('|')[0]
                    bill_type = bill.find('div', class_='field field-name-field-bill-type field-type-entityreference field-label-hidden').text.strip()
                    bill_name = bill.find('div', class_='field-item even').text
                    chamber = 'House' if bill_name[0] == 'H' else 'Senate'
                    try:
                        source_topic = bill.find('div', class_='field field-name-field-subjects field-type-entityreference field-label-hidden').text.strip()
                    except:
                        source_topic = ''

                    info =     {'url':url, 'title': title.text, 'desc': desc, 
                    'last_action':last_action, 'bill_type': bill_type, 'bill_name':bill_name,
                    'chamber':chamber, 'source_topic': source_topic, 'session': code[1]
                    }
                
                    data.append(info)
            except:
                traceback.print_exc()
                print(page_url)

    return data

def votes_detailed( url):
    page = scraper_utils.request(url)
    scraper_utils.crawl_delay(crawl_delay)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find_all('table')
    
    
    total = table[1]
    total_pd = pd.read_html(str(total))[0]
    total_pd = total_pd.to_dict('records')
    
    votes = table[2]
    votes = pd.read_html(str(votes))[0]
    votes.replace('Y', 'yea')
    votes.replace('N', 'nay')
    votes.replace('A', 'absent')
    votes.replace('E', 'absent')
    votes = votes.to_dict('records')


    try:
        len_table = len(table)
        for x in range(3, len_table):
            votes2 = table[x]
            votes2 = pd.read_html(str(votes2))[0]
            votes2.replace('Y', 'yea')
            votes2.replace('N', 'nay')
            votes2.replace('A', 'absent')
            votes2.replace('E', 'absent')
            votes2 = votes2.to_dict('records')[1:]

            votes = votes + votes2


    except IndexError:
        print(votes)
        traceback.print_exc()
        # no second table of legislators
    
    total_pd = {'aye':total_pd[1][1], 'nay':total_pd[1][3], 'absent': total_pd[2][1] + total_pd[2][3],
               'total': total_pd[1][1]+ total_pd[1][3] }
    
    return  votes, total_pd
    

def scrape(data):
    try:

        row = scraper_utils.initialize_row()

    
        url = data['url']
        row.source_url = url
        row.bill_title = data['title']
        row.bill_description = data['desc']
        row.last_action = data['last_action']
        row.bill_type = data['bill_type']
        row.bill_name = data['bill_name']
        row.source_id = data['bill_name']
        row.chamber_origin = data['chamber']
        row.source_topic = data['source_topic']
        row.session = data['session']
        row.goverlytics_id = 'CO_' + data['session'] + '_' + data['bill_name']

        year = data['session'].split(' ', 1)[0]

        page = scraper_utils.request(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find('div', id='bill-documents-tabs8').find('table')

        for br in table.find_all("br"):
            br.replace_with(", ")
            
        pd_table = pd.read_html(str(table))[0]
        pd_table = pd_table.to_dict('records')

        sponsors = pd_table[0]['Legislators'].strip(',').split(',')
        sponsors = [x.strip().split(' ', 2) for x in sponsors]

        try:
            pdf_link = soup.find('div', class_='recent-item recent-bill-text').find_all('a')[0]['href']
            response = requests.get(
                pdf_link, stream=True, headers=scraper_utils._request_headers)
            pdf = pdfplumber.open(io.BytesIO(response.content))
            page = pdf.pages[0]
            row.bill_text = page.extract_text()
        except:
            traceback.print_exc()
            print(url)
            print(pdf_link)

        try:
            sponsors2 = pd_table[1]['Legislators'].strip(',').split(',')
            sponsors2 = [x.strip().split(' ', 2) for x in sponsors2]
            sponsor = sponsors + sponsors2
            row.sponsors_id = [scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', x[1][0], **{'name_last': x[-1]}) 
                                for x in sponsor]
            row.sponsors = [x[-1] for x in sponsor]

        except AttributeError:
            row.sponsors = [x[-1] for x in sponsors]
            row.sponsors_id = [scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', x[1][0], **{'name_last': x[-1]}) 
                                for x in sponsors]
            pass
        


        try:
            co_sponsors = pd_table[2]['Legislators'].strip(',').split(',')
            co_sponsors = [x.strip().split(' ', 2) for x in co_sponsors]
            row.cosponsors_id = [scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', x[1][0], **{'name_last': x[-1]}) 
                                for x in co_sponsors]
            row.cosponsors = [x[-1] for x in co_sponsors]
        
        except AttributeError:
            row.cosponsors = []


        row.current_status = soup.find_all('div', class_='field field-name-field-label field-type-text field-label-hidden')[-1].text.strip()

        try:
            c = soup.find('div', class_='committee-item')
            chambers = c.find_all('div')
            committees = c.find_all('h4')
            for chamber, committee in zip(chambers, committees):
                row.committees.append({'chamber':chamber.text.strip(), 'committee':committee.text.strip()})
        except (AttributeError, ValueError):
            # traceback.print_exc()
            # print(url)
            # no committees, prob resolution
            pass
            

        try:
            history = soup.find('div', id='bill-documents-tabs7').find('table')
            history = pd.read_html(str(history))[0]
            history = history.replace({np.nan: None})
            history = history.to_dict('records')
            
            
            row.date_introduced = history[-1]['Date']
            for h in history:
                row.actions.append({'date': h['Date'], 'action_by': h['Location'], 'description': h['Action']})
        except:
            traceback.print_exc()
            print(url)

        try:
            row.bill_summary = soup.find('div', id='bill-summary-top').text.strip()
        except:
            pass

        try:
            votes = []
            votes_table = soup.find('div', id='bill-documents-tabs4').find_all('table')
            for index, table in enumerate(votes_table):
                votes_pd = pd.read_html(str(table))[0]
                votes_pd = votes_pd.to_dict('records')
                pdf_urls = table.find_all('a')
                for rows, vote in enumerate(votes_pd):
                    if index == 0:
                        chamber =  'Senate'
                    else: 
                        chamber = 'House'
                    info = {'date': vote['Date'],
                    'description': vote['Action'], 
                    'chamber': chamber
                    }
                    try:
                        vote_count, total = votes_detailed( base_url + pdf_urls[rows]['href'])
                    except:
                        # only 1 table of votes
                        pass
                        # traceback.print_exc()

                        

                    vote_count = vote_count[1:]
                    for vote in vote_count:
                        try:
                            name_last = vote[0]
                            if '.' in name_last:
                                name_last, first_initial = name_last.split(' ', 1)

                                votes.append(
                                    {'goverlytics_id': scraper_utils.legislators_search_startswith('goverlytics_id', 
                                    'name_first', first_initial.replace('.', ''), **{'name_last': name_last.strip()}) , 'legislator': vote[0], 'vote_text': vote[1]})
                            else:
                                gov_id = {'goverlytics_id': scraper_utils.get_legislator_id(
                                    **{'name_last': vote[0], 'most_recent_term_id' : int(year)}), 'legislator': vote[0], 'vote_text': vote[1]}
                                if gov_id['goverlytics_id'] == None:
                                    gov_id = {'goverlytics_id': scraper_utils.get_legislator_id(
                                        **{'name_last': vote[0]}), 'legislator': vote[0], 'vote_text': vote[1]}
                                votes.append(gov_id)
                        except (IndexError, ValueError):
                            traceback.print_exc()
                            print(url)

                    
                    row.votes.append({'date': info['date'],
                                    'description': info['description'], 
                                    'yea': int(total['aye']), 'nay': int(total['nay']), 'nv': 0, 'absent': int(total['absent']), 'total': int(total['total']),
                                    'passed': total['aye'] > math.floor(total['total']/2),
                                    'chamber': info['chamber'], 
                                    'votes': votes})

                
        except AttributeError:
            # no votes yet for this bill
            pass


        scraper_utils.crawl_delay(crawl_delay)
    except:
        traceback.print_exc()
        print(url)

    return row


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:

    urls = get_urls()
    # Next, we'll scrape the data we want to collect from those URLs.
    # Here we can use Pool from the multiprocessing library to speed things up.
    # We can also iterate through the URLs individually, which is slower:
    # data = [scrape(url) for url in urls]  
    
    with Pool() as pool:
        data = list(tqdm(pool.imap(scrape, urls), position=0, leave=True , total=len(urls)))

    # Once we collect the data, we'll write it to the database.
    scraper_utils.write_data(data)

    print('Complete!')
