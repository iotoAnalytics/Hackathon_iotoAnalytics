'''
Scraper for collecting Canadian federal legislator (ie: MP, or Member of Parliament) data.
A Senator scraper has also been added. Switches have been added that enable you to choose
whether you want to scrape either MPs, Senators, or both.
Author: Justin Tendeck
Notes:
    Currently, this scraper just collects the most recent data, but it looks like they have
        a swath of historical data we can come back for. It would be especially useful for
        time series analysis. Eg: https://www.ourcommons.ca/members/en/wayne-easter(43)/roles
        (that can be accessed by clicking the link under "All Roles" on an MP's page).

Known Issues:
    Everything seemed to be working fine when ran the first time but I tried running it the
        next day and received an error from psycopg2 saying the cursor had already been 
        closed when trying to insert the data. Breaking the data into chunks of 100 or
        so datapoints seemed to remedy the issue.
'''
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))

from scraper_utils import CAFedLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import pandas as pd
import xml.etree.ElementTree as ET
import traceback
from tqdm import tqdm

scraper_utils = CAFedLegislatorScraperUtils()


scrape_mps = True
scrape_senators = True
write_results_to_database = True

mp_base_url = 'https://www.ourcommons.ca'
sen_base_url = 'https://sencanada.ca'

# Both have sen and MP will have the same scraper delay
crawl_delay = scraper_utils.get_crawl_delay(mp_base_url)

# Initialized in the get_mp_basic_details() method.
mp_df = pd.DataFrame()
# Initialized in the get_sen_basic_details() method.
sen_df = pd.DataFrame()

# Used to swap parties with database representation
mp_party_switcher = {
    'NDP': 'New Democratic',
    'Green Party': 'Green'
}

sen_party_switcher = {
    'PSG': 'Progressive Senate Group',
    'C': 'Conservative',
    'ISG': 'Independent Senators Group',
    'CSG': 'Canadian Senators Group'
}

# region MP Scraper Functions


def get_mp_basic_details():
    """
    Get details about MP tile card located at:
    https://www.ourcommons.ca/members/en/search
    Also initializes the mp_df
    """
    global mp_df
    mp_list_url = f'{mp_base_url}/members/en/search'

    page = scraper_utils.request(mp_list_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    mp_tiles = soup.find('div', {'id': 'mip-tile-view'})

    mp_data = []
    for tile in mp_tiles.findAll('div', {'class': 'ce-mip-mp-tile-container'})[:50]:
        row = scraper_utils.initialize_row()

        mp_url = tile.find('a', {'class': 'ce-mip-mp-tile'}).get('href')

        row.source_url = f'{mp_base_url}{mp_url}'
        row.source_id = mp_url.split('(')[-1][:-1]

        name_suffix = tile.find(
            'div', {'class': 'ce-mip-mp-honourable'}).text.strip()
        name_suffix = 'Hon.' if name_suffix == 'The Honourable' else name_suffix
        row.name_full = tile.find('div', {'class': 'ce-mip-mp-name'}).text
        hn = HumanName(row.name_full)
        row.name_last = hn.last
        row.name_first = hn.first
        row.name_middle = hn.middle
        row.name_suffix = hn.suffix if hn.suffix else name_suffix

        party = tile.find('div', {'class': 'ce-mip-mp-party'}).text
        row.party = mp_party_switcher[party] if party in mp_party_switcher else party
        row.party_id = scraper_utils.get_party_id(row.party)

        row.riding = tile.find('div', {'class': 'ce-mip-mp-constituency'}).text
        province_territory = tile.find(
            'div', {'class': 'ce-mip-mp-province'}).text
        row.province_territory = scraper_utils.get_prov_terr_abbrev(
            province_territory)
        row.province_territory_id = scraper_utils.get_prov_terr_id(
            row.province_territory)
        row.region = scraper_utils.get_region(row.province_territory)

        row.role = 'MP'

        mp_data.append(row)
    scraper_utils.crawl_delay(crawl_delay)
    mp_df = pd.DataFrame(mp_data)


def get_mp_contact_details(contact_url):
    """
    Get contact details from each MP's contact page.
    Eg: https://www.ourcommons.ca/members/en/ziad-aboultaif(89156)#contact
    Args:
        contact_url: URL for MP's contact page
    Returns:
        contact: dictionary containing contact details, including phone_numberss,
            addresses, and email.
    """
    page = scraper_utils.request(contact_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    container = soup.find('div', {'id': 'contact'})

    contact = {'phone_numbers': [], 'addresses': [], 'email': ''}

    try:
        # Email found in first p tag of contact container
        email = container.find('p').text.strip()
        contact['email'] = email

        # Get House of Commons contact details
        hill_container = container.find('div', {'class': 'col-md-3'})
        hill_ptags = hill_container.findAll('p')
        hill_address = hill_ptags[0].get_text(
            separator=", ").strip().replace('*,', '-').replace(',,', ',')
        hill_phone = hill_ptags[1].get_text(
            separator=" ").strip().split(' ')[1]

        contact['addresses'].append(
            {'location': 'House of Commons', 'address': hill_address})
        contact['phone_numbers'].append(
            {'location': 'House of Commons', 'number': hill_phone})

        # Get constituency contact details. MP may have multiple constituency offices.
        con_containers = container.findAll(
            'div', {'class': 'ce-mip-contact-constituency-office'})
        for con_container in con_containers:
            office_name = con_container.strong.extract().get_text()
            office_name = ' '.join(office_name.split())
            con_ptags = con_container.findAll('p')
            con_address = con_ptags[0].get_text().strip()
            con_address = ' '.join(con_address.split())

            con_phone_txt_lst = con_ptags[1].get_text().strip().split(' ')
            con_phone = '' if len(
                con_phone_txt_lst) < 3 else con_phone_txt_lst[2]

            contact['addresses'].append(
                {'location': office_name, 'address': con_address})
            if con_phone:
                contact['phone_numbers'].append(
                    {'location': office_name, 'number': con_phone})
    except Exception:
        print('An error occurred extracting contact information.')
        print(f'Problem URL: {contact_url}')
        print(traceback.format_exc())
    scraper_utils.crawl_delay(crawl_delay)
    return contact


def get_mp_role_details(roles_url):
    """
    Get details about MP's role, including most recent parliamentary session, offices/roles
    held as MP, committees, and Parliamentary associations and interparliamentary group roles.
    Args:
        roles_url: link to MP's XML data page containing role information
    Returns:
        roles: dictionary container MP role details
    """
    page = scraper_utils.request(roles_url)
    tree = ET.fromstring(page.content)

    roles = {'most_recent_term_id': '', 'offices_roles_as_mp': [],
             'committees': [], 'parl_assoc_interparl_groups': []}

    roles['most_recent_term_id'] = tree.find('CaucusMemberRoles').find('CaucusMemberRole') \
        .find('ParliamentNumber').text

    for parl_pos_role in tree.find('ParliamentaryPositionRoles').findall('ParliamentaryPositionRole'):
        roles['offices_roles_as_mp'].append(parl_pos_role.find('Title').text)

    for committee in tree.find('CommitteeMemberRoles').findall('CommitteeMemberRole'):
        role = committee.find('AffiliationRoleName').text
        com = committee.find('CommitteeName').text
        roles['committees'].append({'role': role, 'committee': com})

    for paigr in tree.find('ParliamentaryAssociationsandInterparliamentaryGroupRoles') \
            .findall('ParliamentaryAssociationsandInterparliamentaryGroupRole'):
        role = paigr.find('AssociationMemberRoleType').text
        title = paigr.find('Title').text if paigr.find('Title').text else ''
        organization = paigr.find('Organization').text
        roles['parl_assoc_interparl_groups'].append(
            {'role': role, 'title': title, 'organzation': organization})
    scraper_utils.crawl_delay(crawl_delay)
    return roles


def get_mp_fine_details():
    """
    Get more specific details from each MP's profile page.
    Eg: https://www.ourcommons.ca/members/en/ziad-aboultaif(89156)
    """
    global mp_df

    for i, row in tqdm(mp_df.iterrows()):
        contact_url = f"{row['source_url']}#contact"
        contact = get_mp_contact_details(contact_url)
        mp_df.at[i, 'email'] = contact['email']
        mp_df.at[i, 'addresses'] = contact['addresses']
        mp_df.at[i, 'phone_numbers'] = contact['phone_numbers']

        roles_url = f"{row['source_url']}/xml"
        roles = get_mp_role_details(roles_url)
        mp_df.at[i, 'most_recent_term_id'] = roles['most_recent_term_id']
        mp_df.at[i, 'offices_roles_as_mp'] = roles['offices_roles_as_mp']
        mp_df.at[i, 'committees'] = roles['committees']
        mp_df.at[i, 'parl_assoc_interparl_groups'] = roles['parl_assoc_interparl_groups']

        wiki_url = f"https://en.wikipedia.org/wiki/{row['name_first']}_{row['name_last']}"
        wiki_data = scraper_utils.scrape_wiki_bio(wiki_url)
        mp_df.at[i, 'birthday'] = wiki_data['birthday']
        mp_df.at[i, 'education'] = wiki_data['education']
        mp_df.at[i, 'occupation'] = wiki_data['occupation']
        mp_df.at[i, 'years_active'] = wiki_data['years_active']
        mp_df.at[i, 'most_recent_term_id'] = wiki_data['most_recent_term_id']


def mp_scrape():
    """
    Entry point for scraper. Begins by collecting details directly from House of Commons
    website MP roster (get_mp_basic_details), then collects details from each MP's
    individual HoC page (get_mp_fine_details). Data is stored in a dataframe, which
    is accessible globally.
    """
    get_mp_basic_details()
    get_mp_fine_details()
# endregion

# region Senator Scraper Functions


def get_sen_basic_details():
    """
    Collects details about each senator from the Canada Senate roster.
    Ie: https://sencanada.ca/en/senators-list/
    Instantiates senator dataframe (sen_df) and populates it with 
    senator data.
    """
    global sen_df
    sen_page_url = f'{sen_base_url}/en/senators-list/'
    page = scraper_utils.request(sen_page_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    sen_table = soup.find('table', {'id': 'senator-list-view-table'})
    sen_tbody = sen_table.tbody

    sen_data = []
    for tr in sen_tbody.findAll('tr'):
        row = scraper_utils.initialize_row()

        tds = tr.findAll('td')

        # Name details
        name_full = tds[0].get_text().strip()
        hn = HumanName(name_full)
        row.name_full = str(hn).replace(' P.C.', '')
        row.name_last = hn.last
        row.name_first = hn.first
        row.name_middle = hn.middle
        row.name_suffix = hn.suffix

        # Source url
        source_url = f"{sen_base_url}{tds[0].a.get('href')}"
        row.source_url = source_url

        # Party, party ID
        party = tds[1].get_text().strip()
        party = sen_party_switcher[party] if party in sen_party_switcher else party
        row.party = party
        row.party_id = scraper_utils.get_party_id(party)

        # Province/Territory, ID
        prov_terr = tds[2].get_text().strip()
        prov_terr = scraper_utils.get_prov_terr_abbrev(prov_terr)
        row.province_territory = prov_terr
        row.province_territory_id = scraper_utils.get_prov_terr_id(prov_terr)
        row.region = scraper_utils.get_region(prov_terr)

        row.role = 'Senator'
        row.offices_roles_as_mp = None
        row.parl_assoc_interparl_groups = None

        sen_data.append(row)
    scraper_utils.crawl_delay(crawl_delay)
    sen_df = pd.DataFrame(sen_data)


def get_individual_sen_page_details(sen_page_url):
    """
    Collects details from individual senator pages. This includes senator
    contact details (phone number, email) and committee information.
    Args:
        sen_page_url: URL for a given senator's page
    Returns:
        sen_details: Senator detail dictionary contacting phone_numbers,
            email, and committees
    """
    page = scraper_utils.request(sen_page_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    sen_details = {'phone_numbers': [], 'email': '', 'committees': []}

    bio_card = soup.find('ul', {'class': 'biography_card_details'})

    tele_con = bio_card.find(
        'li', {'class': 'biography_card_details_telephone'})
    tele_con.strong.extract()
    telephone = tele_con.get_text().strip()
    sen_details['phone_numbers'].append(
        {'number': telephone, 'location': 'Senate'})

    email_con = bio_card.find('li', {'class': 'biography_card_details_email'})
    email_con.strong.extract()
    email = email_con.get_text().strip()
    sen_details['email'] = email

    try:
        news = soup.find('div', {'class': 'news section'})
        committee_carousel = news.find(
            'div', {'id': 'committee-senator-carousel-container'})
        if committee_carousel:
            for car_item in committee_carousel.findAll('div', {'class': 'home-carousel-item'}):
                role = car_item.find(
                    'div', {'class': 'media-box_date'}).get_text().strip()
                committee = car_item.find(
                    'div', {'class': 'hidden-sm'}).get_text().strip()
                sen_details['committees'].append(
                    {'role': role, 'committee': committee})
    except:
        pass
    scraper_utils.crawl_delay(crawl_delay)
    return sen_details


def get_sen_fine_details():
    """
    Collects finer details for each senator from their individual pages.
    """
    global sen_df

    for i, row in sen_df.iterrows():
        sen_details = get_individual_sen_page_details(row['source_url'])
        sen_df.at[i, 'phone_numbers'] = sen_details['phone_numbers']
        sen_df.at[i, 'email'] = sen_details['email']
        sen_df.at[i, 'committees'] = sen_details['committees']

        wiki_url = f"https://en.wikipedia.org/wiki/{row['name_first']}_{row['name_last']}"
        wiki_data = scraper_utils.scrape_wiki_bio(wiki_url)
        sen_df.at[i, 'birthday'] = wiki_data['birthday']
        sen_df.at[i, 'education'] = wiki_data['education']
        sen_df.at[i, 'occupation'] = wiki_data['occupation']
        sen_df.at[i, 'years_active'] = wiki_data['years_active']
        sen_df.at[i, 'most_recent_term_id'] = wiki_data['most_recent_term_id']


def senator_scrape():
    """
    Entry point for senator page scraper.
    """
    get_sen_basic_details()
    get_sen_fine_details()

# endregion


if __name__ == '__main__':
    # Switches to determine what to scrape located at top of file.
    if scrape_senators:
        print('Collecting senator data...')
        senator_scrape()
    else:
        print('scrape_senators switch is false. Skipping senator data collection...')
    if scrape_mps:
        print('Collecting MP data...')
        mp_scrape()
    else:
        print('scrape_mps switch is false. Skipping MP data collection...')

    # Coalesce dataframes
    result = pd.concat([mp_df, sen_df])

    if write_results_to_database and not result.empty:
        print('Writing data to database...')
        scraper_utils.write_data(result.to_dict('records'))
    else:
        print('Either write to database switch set to false or no data collected. No data written to database.')

    print('Complete!')
