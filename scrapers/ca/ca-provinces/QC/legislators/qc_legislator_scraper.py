import pandas as pd
import bs4
from urllib.request import urlopen as uReq
from urllib.request import Request
from bs4 import BeautifulSoup as soup
import psycopg2
from nameparser import HumanName
import requests
import datefinder
import unidecode
from multiprocessing import Pool
import datetime
import re
import numpy as np
from datetime import datetime
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[4]

sys.path.insert(0, str(p))
from legislator_scraper_utils import CAProvTerrLegislatorScraperUtils


scraper_utils = CAProvTerrLegislatorScraperUtils('QC', 'ca_qc_legislators')

def getAssemblyLinks(myurl):
    infos = []
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")

    table = page_soup.find("table", {"id": "ListeDeputes"})
    trs = table.findAll("tr")[1:]
    for tr in trs:
        link = "http://www.assnat.qc.ca/" + tr.td.a["href"]
        infos.append(link)
    return infos


def collect_leg_data(myurl):
    req = Request(myurl,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    page_soup = soup(webpage, "html.parser")
    img = page_soup.findAll("img")
    name = (img[12]["alt"])
    hn = HumanName(name)

    member_id = myurl.split("/index")[0]
    member_id = member_id.split("-")
    member_id = member_id[len(member_id) - 1]

    personal_info = page_soup.find("div", {"class": "enteteFicheDepute"})
    personal_info = personal_info.findAll("li")
    riding = personal_info[0].text
    riding = riding.split("for ")[1]
    party = personal_info[1].text.strip()
    psparties_lower = []
    # for psp in dbwork.psparties:
    #     unaccented_string = unidecode.unidecode(psp)
    #     psparties_lower.append(unaccented_string.lower())
    # party_unaccented = unidecode.unidecode(party)
    # if party_unaccented.lower() in psparties_lower:
    #     index = psparties_lower.index(party_unaccented.lower())
    #
    #     party_id = dbwork.psids[index]
    # else:
    #     print(party)

    committees = []
    uls = page_soup.findAll("ul")
    committee_offices = []
    for ul in uls:
        try:
            if ul.h4.text == "Current Offices":
                offices = ul.findAll("li")
                for office in offices:
                    committee_offices.append(office.text)
        except:
            pass
    for co in committee_offices:
        if " of the " in co:
            co = co.split(" of the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee, 'house': 'National Assembly'}

            committees.append(com_info)
        elif " to the " in co:
            co = co.split(" to the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()

            committee = committee.replace("Minister of", "").strip()

            com_info = {'role': role, 'committee': committee, 'house': 'National Assembly'}

            committees.append(com_info)
        elif " for " in co:
            co = co.split(" for ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee, 'house': 'National Assembly'}
            committees.append(com_info)

        elif " of " in co:
            co = co.split(" of ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee, 'house': 'National Assembly'}

            committees.append(com_info)
        elif " on the " in co:
            co = co.split(" on the ")
            role = co[0]
            committee = co[1]
            if "since" in committee:
                committee = co[1].split("since")[0].strip()
            committee = committee.split("from")[0].strip()
            com_info = {'role': role, 'committee': committee, 'house': 'National Assembly'}

            committees.append(com_info)



    contact_link = myurl.replace("index", "coordonnees")

    req = Request(contact_link,
                  headers={'User-Agent': 'Mozilla/5.0'})
    webpage = uReq(req).read()

    uReq(req).close()

    contact_soup = soup(webpage, "html.parser")
    address_info = contact_soup.findAll("div", {"class": "blockAdresseDepute"})
    phone_number = []
    numbers = []
    addresses = []

    for adin in address_info:
        try:
            office = adin.h3.text
            alist = (adin.address.text.split("\n"))
            tele = 0
            addr_list = []
            for a in alist:


                if "Telephone: " in a:
                    tele = 1
                    number = a.replace("Telephone: ", "").strip()
                    number = number.split(" ")[0]


                    if number not in numbers:
                        numbers.append(number)
                        num_info = {'office': office, 'number': number}
                        phone_number.append(num_info)
                elif "Fax" in a:
                    tele = 1
                elif "Toll" in a:
                    tele = 1

                if tele == 0:
                    addr_line = a.replace("\r", "")
                    addr_line = " ".join(addr_line.split())
                    if addr_line.strip() != "":
                        addr_list.append(addr_line.strip())

            address = ', '.join(addr_list)
            addr_info = {'location': office, 'address': address}
            addresses.append(addr_info)

        except:
            pass


    email = ""
    try:
        email = (address_info[2].address.a["href"]).replace("mailto:", "")

    except:
        try:
            email = (address_info[0].address.a["href"]).replace("mailto:", "")
        except:
            try:
                email = (address_info[1].address.a["href"]).replace("mailto:", "")
            except:
                pass
    capitalized_party = party.title()

    try:
        party_id = scraper_utils.get_party_id(capitalized_party)
    except:
        party_id = 0

    info = {'province_url': myurl, 'member_id': member_id, 'role': 'Member of National Assembly', 'name_full': name,
            'name_first': hn.first, 'name_last': hn.last, 'name_suffix': hn.suffix, 'name_middle': hn.middle,
            'riding': riding, 'party': party, 'party_id': party_id, 'email': email, 'committees': committees,
            'phone_number': phone_number, 'addresses': addresses, 'military_experience': ""}
    return info


def get_wiki_people(repLink):
    bio_lnks = []
    uClient = uReq(repLink)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    tables = page_soup.findAll("tbody")
    people = tables[1].findAll("tr")
    i = 0
    for person in people[1:]:
        try:
            info = person.findAll("td")

            biolink = "https://en.wikipedia.org/" + (info[1].span.span.span.a["href"])

            bio_lnks.append(biolink)

        except:
            pass
        i += 1
    # print(bio_lnks)
    # print(len(bio_lnks))

    return bio_lnks


# def find_wiki_data(repLink):
#     most_recent_term_id = 0
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         reps = page_soup.find("div", {"class": "mw-parser-output"})
#         repBirth = reps.find("span", {"class": "bday"}).text
#
#         b = datetime.datetime.strptime(repBirth, "%Y-%m-%d").date()
#
#         birthday = b
#         # print(b)
#
#
#
#
#     except:
#         # couldn't find birthday in side box
#         birthday = None
#
#     # get years_active, based off of "assumed office"
#     years_active = []
#     year_started = ""
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         table = page_soup.find("table", {"class": "infobox vcard"})
#
#         tds = table.findAll("td", {"colspan": "2"})
#         td = tds[0]
#
#         for td in tds:
#             asof = (td.find("span", {"class": "nowrap"}))
#             if asof != None:
#                 if (asof.b.text) == "Assumed office":
#
#                     asofbr = td.find("br")
#
#                     year_started = (asofbr.nextSibling)
#
#                     year_started = year_started.split('[')[0]
#                     if "," in year_started:
#                         year_started = year_started.split(',')[1]
#                     year_started = (year_started.replace(" ", ""))
#                     year_started = re.sub('[^0-9]', '', year_started)
#                     if year_started.startswith("12"):
#                         year_started = year_started.substring(1)
#
#
#
#                 else:
#                     pass
#
#     except Exception as ex:
#
#         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#         message = template.format(type(ex).__name__, ex.args)
#         # print(message)
#
#     if year_started != "":
#         years_active = list(range(int(year_started), 2021))
#         # years_active_lst.append(years_active_i)
#     else:
#         years_active = []
#         # years_active_i = []
#         # years_active_i.append(years_active)
#         # years_active_lst.append(years_active_i)
#
#     # get education
#     education = []
#     lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD", "DDS", "MS", "BS", "MBA", "MS", "MD"]
#
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         reps = page_soup.find("div", {"class": "mw-parser-output"})
#         # repsAlmaMater = reps.find("th", {"scope:" "row"})
#         left_column_tags = reps.findAll()
#         lefttag = left_column_tags[0]
#         for lefttag in left_column_tags:
#             if lefttag.text == "Alma mater" or lefttag.text == "Education":
#                 index = left_column_tags.index(lefttag) + 1
#                 next = left_column_tags[index]
#                 alines = next.findAll()
#                 for aline in alines:
#                     if "University" in aline.text or "College" in aline.text or "School" in aline.text:
#                         school = aline.text
#                         # this is most likely a school
#                         level = ""
#                         try:
#                             lineIndex = alines.index(aline) + 1
#                             nextLine = alines[lineIndex].text
#                             if re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
#                                 level = nextLine
#                         except:
#                             pass
#
#                     edinfo = {'level': level, 'field': "", 'school': school}
#
#                     if edinfo not in education:
#                         education.append(edinfo)
#
#     except Exception as ex:
#
#         template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#
#         message = template.format(type(ex).__name__, ex.args)
#
#         # print(message)
#
#     # get full name
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         head = page_soup.find("h1", {"id": "firstHeading"})
#         name = head.text
#         name = name.replace(" (politician)", "")
#         name = name.replace(" (Canadian politician)", "")
#         name = name.replace(" (Quebec politician)", "")
#
#
#     except:
#         name = ""
#     name = unidecode.unidecode(name)
#
#     hN = HumanName(name)
#
#     # get occupation
#     occupation = []
#
#     try:
#         uClient = uReq(repLink)
#         page_html = uClient.read()
#         uClient.close()
#         # # html parsing
#         page_soup = soup(page_html, "html.parser")
#
#         # #
#         # # #grabs each product
#         reps = page_soup.find("div", {"class": "mw-parser-output"})
#
#         left_column_tags = reps.findAll()
#         lefttag = left_column_tags[0]
#         for lefttag in left_column_tags:
#             if lefttag.text == "Occupation":
#                 index = left_column_tags.index(lefttag) + 1
#                 occ = left_column_tags[index].text
#                 if occ != "Occupation":
#                     occupation.append(occ)
#
#     except:
#         pass
#
#     most_recent_term_id = ""
#     try:
#         most_recent_term_id = (years_active[len(years_active) - 1])
#
#     except:
#         pass
#
#     info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
#             'education': education, 'occupation': occupation, 'years_active': years_active,
#             'most_recent_term_id': str(most_recent_term_id)}
#
#     # print(info)
#     return info


assembly_link = "http://www.assnat.qc.ca/en/deputes/index.html"

assembly_members = getAssemblyLinks(assembly_link)
# assembly_members = assembly_members[20:]
assembly_members = assembly_members
# print(len(assembly_members))
if __name__ == '__main__':
    with Pool() as pool:
        leg_data = pool.map(func=collect_leg_data, iterable=assembly_members)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    leg_df = pd.DataFrame(leg_data)
    # print(leg_df)

    wiki_link = 'https://en.wikipedia.org/wiki/National_Assembly_of_Quebec'
    wiki_people = get_wiki_people(wiki_link)

    with Pool() as pool:
        wiki_data = pool.map(func=scraper_utils.scrape_wiki_bio, iterable=wiki_people)
    wiki_df = pd.DataFrame(wiki_data)

    print(wiki_df)
    mergedRepsData = pd.merge(leg_df, wiki_df, how='left', on=["name_first", "name_last"])
    mergedRepsData['most_recent_term_id'] = mergedRepsData['most_recent_term_id'].replace({np.nan: None})
    mergedRepsData['years_active'] = mergedRepsData['years_active'].replace({np.nan: None})
    mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({np.nan: None})
    mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({np.nan: None})
    mergedRepsData['education'] = mergedRepsData['education'].replace({np.nan: None})
    big_df = mergedRepsData
    big_df['seniority'] = 0



    sample_row = scraper_utils.initialize_row()

    #

    big_df['province_territory'] = sample_row.province_territory
    big_df['province_territory_id'] = sample_row.province_territory_id
    #
    #
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id
    big_df['source_url'] = big_df['province_url']

    big_df['source_id'] = big_df['member_id']

    print(big_df)

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')

