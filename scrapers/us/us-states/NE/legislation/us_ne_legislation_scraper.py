import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules

p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))
from scraper_utils import USStateLegislationScraperUtils
from bs4 import BeautifulSoup
import requests
import io
from multiprocessing import Pool
from datetime import date
import re
import pdfplumber

state_abbreviation = 'NE'
database_table_name = 'us_ne_legislation_test'
legislator_table_name = 'us_ne_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)

date = date.today().year
base_url = 'https://nebraskalegislature.gov/'
bill_url = f'{base_url}bills/search_by_date.php?SessionDay={date}'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

session_soup = BeautifulSoup(requests.get(base_url).content, 'lxml')
session_string = session_soup.find('div', {'class': 'main-content'}).find('h2').text.split(',')[0].split()[1]
session = re.search('[0-9]+', session_string).group()

date_dict = {
    'January': '01',
    'Jan': '01',
    'February': '02',
    'Feb': '02',
    'March': '03',
    'Mar': '03',
    'April': '04',
    'Apr': '04',
    'May': '05',
    'June': '06',
    'Jun': '06',
    'July': '07',
    'Jul': '07',
    'August': '08',
    'Aug': '08',
    'September': '09',
    'Sep': '09',
    'Sept': '09',
    'October': '10',
    'Oct': '10',
    'November': '11',
    'Nov': '11',
    'December': '12',
    'Dec': '12'
}


def edit_sponsors(sponsor_string):
    colon_split = sponsor_string.split(':')
    if len(colon_split) == 1:
        return colon_split[0].strip()
    return colon_split[1].strip()


def scrape_bill_table_row(row_html):
    td_lst = row_html.find_all('td')
    bill_link = td_lst[0].find('a').get('href')
    return {
        'link': f'{base_url + bill_link}',
        'primary_sponsor': edit_sponsors(td_lst[1].text),
        'status': td_lst[2].text.strip(),
        'description': td_lst[3].text.strip()
    }


def edit_date(date_string):
    date_split = [x.replace(',', '').strip() for x in date_string.split()]
    return f'{date_split[2]}-{date_dict[date_split[0]]}-{date_split[1]}'


def make_bill_dicts(bill_link_url):
    soup = BeautifulSoup(requests.get(bill_link_url).content, 'lxml')
    table = soup.find('div', {'class': 'table-responsive'}).find('table')
    return [scrape_bill_table_row(x) for x in table.find_all('tr')[1:]]


def get_id(legislator):
    name_split = legislator.split()
    if len(name_split) == 2:
        if name_split[1] == 'Chairperson':
            return scraper_utils.get_legislator_id(name_last=name_split[0].replace(',', '').strip())
        elif ',' in name_split[0]:
            initial = name_split[1].replace('.', '').strip()
            name_last = name_split[0].replace(',', '').strip()
            gov_id = scraper_utils.legislators_search_startswith('goverlytics_id', 'name_first', initial,
                                                                 name_last=name_last)
            return gov_id
        else:
            gov_id = scraper_utils.get_legislator_id(name_last=name_split[1], name_middle=name_split[0])
            return gov_id
    elif len(name_split) == 1:
        return scraper_utils.get_legislator_id(name_last=name_split[0])
    elif ':' in legislator:
        return scraper_utils.get_legislator_id(name_last=name_split[2])
    else:
        coms_lst = [x.replace('Committee', '').strip() for x in legislator.split(',')]
        coms_lst = [{'chamber': 'Senate', 'committee': x} for x in coms_lst]
        return coms_lst


def scrape_vote_page(vote_url):
    vote_soup = BeautifulSoup(requests.get(vote_url).content, 'lxml')
    vote_tables = vote_soup.find_all('table', {'class': 'table table-sm mb-0'})
    vote_tally_table = vote_tables[0].find_all('li')
    vote_info_dict = {}
    for li in vote_tally_table:
        if 'Yes:' in li.text:
            vote_info_dict['yea'] = int(li.text.split(':')[1].strip())
        elif 'No:' in li.text:
            vote_info_dict['nay'] = int(li.text.split(':')[1].strip())
        elif 'Absent' in li.text:
            vote_info_dict['absent'] = int(li.text.split(':')[1].strip())
        elif 'Not Voting' in li.text:
            if 'nv' not in vote_info_dict:
                vote_info_dict['nv'] = int(li.text.split(':')[1].strip())
            else:
                vote_info_dict['nv'] += int(li.text.split(':')[1].strip())

    summation = 0
    for key, value in vote_info_dict.items():
        summation += value
    vote_info_dict['total'] = summation

    voters = []
    individual_votes_table = vote_tables[1].find_all('tr')
    for legislators in individual_votes_table:
        td_lst = legislators.find_all('td')
        for i in range(0, len(td_lst), 2):
            if td_lst[i].text:
                voters.append({
                    'legislator': td_lst[i].text,
                    'goverlytics_id': get_id(td_lst[i].text),
                    'vote_text': td_lst[i + 1].text
                })
    vote_info_dict['votes'] = voters

    return vote_info_dict


def get_votes(vote_table_html):
    vote_lst, action_lst = [], []
    for vote in vote_table_html:
        vote_info = vote.find_all('td')
        date_string = edit_date(vote_info[0].text)
        action = vote_info[1].text
        try:
            vote_link = 'https://nebraskalegislature.gov/bills/' + vote_info[3].find('a').get('href')
        except AttributeError:
            vote_link = ''
        vote_info = scrape_vote_page(vote_link) if vote_link else ''
        vote_info_dict = {
            'date': date_string,
            'description': action,
            'chamber': 'Senate'
        }
        if vote_info:
            vote_info_dict['yea'] = vote_info['yea']
            vote_info_dict['nay'] = vote_info['nay']
            vote_info_dict['absent'] = vote_info['absent']
            vote_info_dict['nv'] = vote_info['nv']
            vote_info_dict['total'] = vote_info['total']
            vote_info_dict['votes'] = vote_info['votes']
            vote_info_dict['passed'] = 1 if vote_info['yea'] > vote_info['nay'] else 0
            vote_info_dict['chamber'] = 'Senate'
            vote_lst.append(vote_info_dict)
        else:
            vote_info_dict['action_by'] = 'Senate'
            action_lst.append(vote_info_dict)
    return vote_lst, action_lst


def grab_pdf_link(html_soup):
    link_lst = html_soup.find('div', {'class': 'col-3 d-none d-sm-flex'}).find_all('a')
    for item in link_lst:
        if item.text == 'Introduced':
            pdf_link = base_url + item.get("href").replace('.', '').replace('pdf', '.pdf')
            return pdf_link


def get_pdf_text(pdf_link):
    response = requests.get(pdf_link, stream=True)
    pdf = pdfplumber.open(io.BytesIO(response.content))
    text = ''
    for page in pdf.pages:
        text += page.extract_text().replace('\n', ' ')
    return text


def get_sponsors_from_coms(loc):
    legis_table_dict = scraper_utils.legislators.to_dict('records')
    sponsors, sponsor_ids = [], []
    for committee in loc:
        for legislator in legis_table_dict:
            coms_lst = legislator['committees']
            for com_item in coms_lst:
                if com_item['committee'] == committee['committee']:
                    sponsors.append(legislator['name_last'])
                    sponsor_ids.append(legislator['goverlytics_id'])
    return sponsors, sponsor_ids


def scrape(bill_dict):
    row = scraper_utils.initialize_row()
    url = bill_dict['link']
    row.source_url = url
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    main_content_soup = soup.find('div', {'class': 'main-content'})
    bill_name_text = main_content_soup.find('div', {'class': 'row my-2'}).find('h2').text.strip().split('-')
    bill_name = bill_name_text[0].strip()
    row.bill_name = bill_name
    row.bill_title = bill_name_text[1].strip()
    row.session = session
    row.current_status = bill_dict['status']
    row.bill_description = bill_dict['description']
    row.chamber_origin = 'Senate'

    row.goverlytics_id = f'{state_abbreviation}_{session}_{bill_name}'

    document_info_text = main_content_soup.find_all('div', {'class': 'col-sm-5'})[1].find('ul').find_all('a')
    for a_tag in document_info_text:
        if 'Date of Introduction' in a_tag.text:
            row.date_introduced = a_tag.text.split(':')[1].strip()

    vote_and_actions_table = main_content_soup.find('div', {'class': 'table-responsive'}).find('tbody').find_all('tr')
    vote_and_actions = get_votes(vote_and_actions_table)
    row.votes = vote_and_actions[0]
    row.actions = vote_and_actions[1]

    gov_id_checker = get_id(bill_dict['primary_sponsor'])
    if type(gov_id_checker) == list:
        row.committees = gov_id_checker
        search_by_coms_info = get_sponsors_from_coms(gov_id_checker)
        row.sponsors = search_by_coms_info[0]
        row.sponsors_id = search_by_coms_info[1]
    else:
        row.principal_sponsor = bill_dict['primary_sponsor'].replace(', Chairperson', '').strip()
        row.principal_sponsor_id = gov_id_checker

    row.bill_text = get_pdf_text(grab_pdf_link(soup))
    if 'LB' in bill_name:
        row.bill_type = 'Bill'
    elif 'LR' in bill_name:
        row.bill_type = 'Resolution'

    scraper_utils.crawl_delay(crawl_delay)

    print(f'Done row for {bill_name}')

    return row


if __name__ == '__main__':
    bill_dicts = make_bill_dicts(bill_url)

    with Pool() as pool:
        data = pool.map(scrape, bill_dicts[:5])

    scraper_utils.write_data(data)

    print('Complete!')
