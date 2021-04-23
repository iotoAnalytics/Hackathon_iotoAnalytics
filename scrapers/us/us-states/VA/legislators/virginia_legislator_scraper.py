import re
import datetime
from multiprocessing import Pool
import unidecode
import datefinder
import requests
from nameparser import HumanName
import psycopg2
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
import bs4
import pandas as pd
import time
import argparse
import gzip
import numpy as np
import pickle
from legislator_scraper_utils import USStateLegislatorScraperUtils
import sys
import os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))


scraper_utils = USStateLegislatorScraperUtils('VA', 'us_va_legislators')


def get_delegate_links(myurl):
    delegate_infos = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    house_table = page_soup.find(
        "table", {"summary": "Listings of the Virginia House of Delegates"})
    people = house_table.findAll("tr")
    people = people[1:]
    for person in people:
        member_id = (person["id"]).replace("member[", "")
        member_id = member_id.replace("]", "")
        person_link = myurl + "?id=" + member_id
        person_td = person.findAll("td")
        name_full = person_td[0].text
        hn = HumanName(name_full)
        district = person_td[1].text.replace("th", "")
        district = district.replace("nd", "")
        district = district.replace("st", "")
        district = district.replace("rd", "")
        p = person_td[3].text

        party = p
        if p == "D":
            party = "Democrat"
            party_id = 2
        elif p == "R":
            party = "Republican"
            party_id = 3

        capitol_phone = person_td[4].text.replace("(", "")
        capitol_phone = capitol_phone.replace(") ", "-")
        district_phone = person_td[5].text.replace("(", "")
        district_phone = district_phone.replace(") ", "-")
        phone_number = [{'office': 'capitol office', 'number': capitol_phone},
                        {'office': 'district office', 'number': district_phone}]

        email = person_td[6].text

        if "Vacant" not in name_full:
            person_info = {'state_url': person_link, 'state_member_id': member_id, 'name_full': name_full,
                           'name_last': hn.last, 'name_first': hn.first, 'name_middle': hn.middle,
                           'name_suffix': hn.suffix, 'phone_number': phone_number, 'email': email,
                           'party': party, 'party_id': party_id, 'district': district}
            delegate_infos.append(person_info)

    return delegate_infos


def scrape_rep_bio_page(myurl):
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    locations = page_soup.find("td", {"class": "distDescriptPlacement"})
    locations = locations.text.split("- ")[1]
    locations = locations.replace("and", ",")
    locations = locations.replace(";", ",")
    locations = locations.split(",")
    areas_served = []
    for l in locations:
        l = l.strip()
        if l != "":
            areas_served.append(l)

    addresses = []
    education = []
    occupation = []
    military_experience = ""
    committees = []
    lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD",
            "DDS", "MS", "BS", "MBA", "MS", "MD"]
    offices = page_soup.findAll('div', {'class': 'memBioOffice'})
    for office in offices:

        if "Capitol Office" in office.text or "District Office" in office.text:
            capoff = office
            address_lines = capoff.findAll("tr")
            location = address_lines[0].text.strip()
            if "Physical Address" in address_lines[1].text:
                location = "Physical Address"
                addr = address_lines[1].text.split("Mailing Address")[0]
                addr = addr.split(":")[1]
                addr = addr.strip()
                addr = addr.replace("\n\r\n", "")
                addr = addr.replace(" \r\n", ", ")
                address_info = {
                    'location': 'Physical Address', 'address': addr}
                addresses.append(address_info)
                location = "Mailing Address"
                addr = address_lines[1].text.split("Mailing Address:")[1]
                addr = addr.strip() + ", " + address_lines[2].text.strip()
                address_info = {'location': location, 'address': addr}
                addresses.append(address_info)
            else:
                addr = address_lines[1].text.strip(
                ) + "," + address_lines[2].text.strip()
                if "Office" not in address_lines[3].text:
                    addr = addr + address_lines[3].text

                address_info = {'location': location, 'address': addr}
                addresses.append(address_info)
        info_snippets = office.findAll("li")

        for insn in info_snippets:
            if "Occupation" in insn.text:
                occ = insn.text.replace("Occupation/Profession: ", "")
                for o in occ.split(","):
                    occupation.append(o)
            if "Military Service:" in insn.text:
                ms = insn.text.replace("Military Service: ", "")
                military_experience = ms.strip()
            if "Education" in insn.text:
                education_all = insn.text.replace("Education: ", "").strip()
                education_list = education_all.split(")")
                for ed in education_list:
                    level = ""
                    field = ""
                    school = ed
                    try:
                        school = ed.split("(")[0].strip()
                        school = school.replace("\xa0", "")

                        other_info = ed.split(" (")[1]
                        comma_separated = other_info.split(",")
                        for cs in comma_separated:
                            if re.sub('[^a-zA-Z]+', "", cs) in lvls:
                                level = cs
                            elif cs.strip().isdecimal():
                                # this is a year
                                pass
                            else:
                                field = cs
                    except:
                        pass

                    educ = {'field': field, 'level': level, 'school': school}
                    education.append(educ)
    committee_office_tags = (offices[2]).findAll()
    for cot in committee_office_tags:
        try:
            if "committee" in cot["href"]:
                # role = ""
                com_name = cot.text
                role_index = committee_office_tags.index(cot) - 1

                r = committee_office_tags[role_index]
                if ":" in r.text:
                    role = r.text.replace(":", "")
                else:
                    role = ""

                committee_info = {'role': role, 'committee': com_name}
                committees.append(committee_info)
        except:
            pass

    # print(committees)
    rep_info = {'areas_served': areas_served, 'addresses': addresses, 'role': 'Representative', 'country': 'USA',
                'country_id': 1, 'state': 'VA', 'state_id': 51, 'occupation': occupation, 'education': education,
                'military_experience': military_experience, 'seniority': 0, 'committees': committees,
                'state_url': myurl}
    return rep_info


def get_house_wiki_bios(myurl):
    repLinks = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "html.parser")
    wikitable = page_soup.findAll("table")
    members_table = wikitable[6]
    people = members_table.findAll("tr")
    for person in people:
        try:
            person_info = person.findAll("td")

            link = "https://en.wikipedia.org" + person_info[1].a["href"]
            # print(link)
            repLinks.append(link)
        except:
            pass
    return repLinks


def find_wiki_rep_data(repLink):
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})
        repBirth = reps.find("span", {"class": "bday"}).text

        b = datetime.datetime.strptime(repBirth, "%Y-%m-%d").date()

        birthday = b
        # print(b)

    except:
        # couldn't find birthday in side box
        birthday = None

        # get years_active, based off of "assumed office"
    years_active = []
    year_started = ""
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        table = page_soup.find("table", {"class": "infobox vcard"})

        tds = table.findAll("td", {"colspan": "2"})
        td = tds[0]

        for td in tds:
            asof = (td.find("span", {"class": "nowrap"}))
            if asof != None:
                if (asof.b.text) == "Assumed office":

                    asofbr = td.find("br")

                    year_started = (asofbr.nextSibling)

                    year_started = year_started.split('[')[0]
                    if "," in year_started:
                        year_started = year_started.split(',')[1]
                    year_started = (year_started.replace(" ", ""))
                    year_started = re.sub('[^0-9]', '', year_started)
                    if year_started.startswith("12"):
                        year_started = year_started.substring(1)

                else:
                    pass

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        # print(message)

    if year_started != "":
        years_active = list(range(int(year_started), 2021))
        # years_active_lst.append(years_active_i)
    else:
        years_active = []
        # years_active_i = []
        # years_active_i.append(years_active)
        # years_active_lst.append(years_active_i)

    # get full name
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        head = page_soup.find("h1", {"id": "firstHeading"})
        name = head.text
        name = name.replace(" (politician)", "")
        name = name.replace(" (American politician)", "")
        name = name.replace(" (North Carolina politician)", "")

    except:
        name = ""
    name = unidecode.unidecode(name)

    hN = HumanName(name)

    most_recent_term_id = ""
    try:
        most_recent_term_id = str(years_active[len(years_active) - 1])
    except:
        pass

    info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
            'years_active': years_active, 'most_recent_term_id': most_recent_term_id}

    # print(info)
    return info


def find_senate_bio_links(myurl):
    senate_infos = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    people_table = page_soup.find("table", {"class": "viewTable"})
    people = people_table.findAll("tr")[1:]
    for p in people:
        state_url = "https:" + p.td.a["href"]
        state_member_id = state_url.split("id=")[1]

        name_full = p.td.a.u.text
        hn = HumanName(name_full)

        other_infos = p.findAll("td")
        district = other_infos[1].text

        party = other_infos[2].text

        party_id = ""
        if party == "Democrat":
            party_id = 2
        elif party == "Republican":
            party_id = 3

        pocahontas_number = other_infos[3].text.replace("(", "")
        pocahontas_number = pocahontas_number.replace(") ", "-")
        district_number = other_infos[4].text.replace("(", "")
        district_number = district_number.replace("( ", "-")

        phone_number = []
        pn_info = {'office': 'Pocahontas Building',
                   'number': pocahontas_number}
        phone_number.append(pn_info)
        dn_info = {'office': "District Office", 'number': district_number}
        phone_number.append(pn_info)

        person_info = {'state_url': state_url, 'state_member_id': state_member_id, 'name_full': name_full,
                       'name_last': hn.last, 'name_first': hn.first, 'name_middle': hn.middle,
                       'name_suffix': hn.suffix, 'district': district, 'party': party, 'party_id': party_id,
                       'phone_number': phone_number}
        # print(person_info)
        senate_infos.append(person_info)

    return senate_infos


def find_senate_info(myurl):
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    try:
        aside_bar = page_soup.find("div", {"class": "aside"})
        email = aside_bar.find("strong").text

        contents = page_soup.findAll("div", {"class": "contenttext"})
        coms = contents[1].findAll("a")
        committees = []
        for com in coms:
            comname = com.text
            role = ""
            if "(" in (com.nextSibling):
                role = com.nextSibling.replace("(", "")
                role = role.replace(")", "").strip()
            com = {'role': role, 'committee': comname}
            committees.append(com)

        areas_served = []

        blacktext = page_soup.findAll("div", {"class": "lrgblacktext"})

        all_areas = blacktext[1].text.split(",")

        for aa in all_areas:
            if aa.strip() != '':
                areas_served.append(aa.strip())

        addresses = []
        hsections = page_soup.findAll("h2")

        for hs in hsections:
            if hs.text == "Session Office":
                so_address = hs.nextSibling
                add_next_line = so_address
                pn = 0
                while pn == 0:
                    try:
                        add_next_line = add_next_line.nextSibling
                        if "Phone" in add_next_line:
                            pn = 1
                        else:
                            if str(add_next_line).strip() != "":
                                so_address = so_address + " " + \
                                    str(add_next_line).strip()

                    except Exception as ex:

                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

                        message = template.format(type(ex).__name__, ex.args)
                        # print(message)
                        pn = 1
                so_address = so_address.split("Phone")[0]
                so_address = so_address.replace("<br/>", "")
                so_address = so_address.replace("<br>", "")
                so_address = " ".join(so_address.split())
                addr = {'location': 'session office',
                        'address': so_address.replace("\n", "").strip()}

                addresses.append(addr)

        do_address = blacktext[4].text.split("Phone")[0].strip()
        do_address = " ".join(do_address.split())

        addr = {'location': 'district office',
                'address': do_address.replace('\n', '')}
        addresses.append(addr)

        sen_data = {'state_url': myurl, 'email': email, 'country': 'USA', 'country_id': 1, 'state': 'VA', 'state_id': 51,
                    'role': 'Senator', 'committees': committees, 'areas_served': areas_served, 'addresses': addresses,
                    'seniority': 0, 'military_experience': ""}

        return sen_data
    except:

        pass


def get_senate_wiki_bios(myurl):
    senLinks = []
    uClient = uReq(myurl)
    page_html = uClient.read()
    uClient.close()

    page_soup = soup(page_html, "html.parser")
    wikitable = page_soup.findAll("table")
    members_table = wikitable[6]

    closest_class = members_table.find("div", {"class": "div-col"})
    lis = closest_class.findAll("li")
    for person in lis:
        try:
            link = "https://en.wikipedia.org" + person.a["href"]

            senLinks.append(link)
        except:
            pass

    return senLinks


def find_wiki_sen_data(repLink):
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})
        repBirth = reps.find("span", {"class": "bday"}).text

        b = datetime.datetime.strptime(repBirth, "%Y-%m-%d").date()

        birthday = b
        # print(b)

    except:
        # couldn't find birthday in side box
        birthday = None

        # get years_active, based off of "assumed office"
    years_active = []
    year_started = ""
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        table = page_soup.find("table", {"class": "infobox vcard"})

        tds = table.findAll("td", {"colspan": "2"})
        td = tds[0]

        for td in tds:
            asof = (td.find("span", {"class": "nowrap"}))
            if asof != None:
                if (asof.b.text) == "Assumed office":

                    asofbr = td.find("br")

                    year_started = (asofbr.nextSibling)

                    year_started = year_started.split('[')[0]
                    if "," in year_started:
                        year_started = year_started.split(',')[1]
                    year_started = (year_started.replace(" ", ""))
                    year_started = re.sub('[^0-9]', '', year_started)
                    if year_started.startswith("12"):
                        year_started = year_started.substring(1)

                else:
                    pass

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        # print(message)

    if year_started != "":
        years_active = list(range(int(year_started), 2021))
        # years_active_lst.append(years_active_i)
    else:
        years_active = []
        # years_active_i = []
        # years_active_i.append(years_active)
        # years_active_lst.append(years_active_i)

    # get education
    education = []
    lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD",
            "DDS", "MS", "BS", "MBA", "MS", "MD"]

    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})
        # repsAlmaMater = reps.find("th", {"scope:" "row"})
        left_column_tags = reps.findAll()
        lefttag = left_column_tags[0]
        for lefttag in left_column_tags:
            if lefttag.text == "Alma mater" or lefttag.text == "Education":
                index = left_column_tags.index(lefttag) + 1
                next = left_column_tags[index]
                alines = next.findAll()
                for aline in alines:
                    if "University" in aline.text or "College" in aline.text or "School" in aline.text:
                        school = aline.text
                        # this is most likely a school
                        level = ""
                        try:
                            lineIndex = alines.index(aline) + 1
                            nextLine = alines[lineIndex].text
                            if re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
                                level = nextLine
                        except:
                            pass

                    edinfo = {'level': level, 'field': "", 'school': school}

                    if edinfo not in education:
                        education.append(edinfo)

    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)

        # print(message)

    # get full name
    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        head = page_soup.find("h1", {"id": "firstHeading"})
        name = head.text
        name = name.replace(" (politician)", "")
        name = name.replace(" (American politician)", "")
        name = name.replace(" (North Carolina politician)", "")

    except:
        name = ""
    name = unidecode.unidecode(name)

    hN = HumanName(name)

    # get occupation
    occupation = []

    try:
        uClient = uReq(repLink)
        page_html = uClient.read()
        uClient.close()
        # # html parsing
        page_soup = soup(page_html, "html.parser")

        # #
        # # #grabs each product
        reps = page_soup.find("div", {"class": "mw-parser-output"})

        left_column_tags = reps.findAll()
        lefttag = left_column_tags[0]
        for lefttag in left_column_tags:
            if lefttag.text == "Occupation":
                index = left_column_tags.index(lefttag) + 1
                occ = left_column_tags[index].text
                if occ != "Occupation":
                    occupation.append(occ)

    except:
        pass

    info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
            'education': education, 'occupation': occupation, 'years_active': years_active,
            'most_recent_term_id': str(years_active[len(years_active) - 1])}

    # print(info)
    return info


if __name__ == '__main__':
    delegatepage = 'https://virginiageneralassembly.gov/house/members/members.php'
    senatepage = 'https://apps.senate.virginia.gov/Senator/index.php'
    delegateinfos = get_delegate_links(delegatepage)
    del_df = pd.DataFrame(delegateinfos)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # # # print(del_df)
    # #
    delegatelinks = [(d['state_url']) for d in delegateinfos]

    with Pool() as pool:
        rep_bio_data = pool.map(
            func=scrape_rep_bio_page, iterable=delegatelinks)
    rep_bio_df = pd.DataFrame(rep_bio_data)

    rep_df = pd.merge(rep_bio_df, del_df, how='left', on=['state_url'])
    # print(rep_df)

    house_wiki_link = 'https://en.wikipedia.org/wiki/Virginia_House_of_Delegates'
    wiki_del_links = get_house_wiki_bios(house_wiki_link)
    with Pool() as pool:
        rep_wiki_data = pool.map(
            func=find_wiki_rep_data, iterable=wiki_del_links)
    rep_wiki_df = pd.DataFrame(rep_wiki_data)
    mergedRepsData = pd.merge(rep_df, rep_wiki_df, how='left', on=[
                              "name_first", "name_last"])
    mergedRepsData['committees'] = mergedRepsData['committees'].replace({
                                                                        np.nan: None})
    mergedRepsData['seniority'] = mergedRepsData['seniority'].replace({
                                                                      np.nan: None})
    mergedRepsData['military_experience'] = mergedRepsData['military_experience'].replace({
                                                                                          np.nan: None})
    mergedRepsData['areas_served'] = mergedRepsData['areas_served'].replace({
                                                                            np.nan: None})
    mergedRepsData['most_recent_term_id'] = mergedRepsData['most_recent_term_id'].replace({
                                                                                          np.nan: None})
    mergedRepsData['years_active'] = mergedRepsData['years_active'].replace({
                                                                            np.nan: None})
    mergedRepsData['occupation'] = mergedRepsData['occupation'].replace({
                                                                        np.nan: None})
    mergedRepsData['birthday'] = mergedRepsData['birthday'].replace({
                                                                    np.nan: None})
    mergedRepsData['education'] = mergedRepsData['education'].replace({
                                                                      np.nan: None})

    senate_bio_infos = find_senate_bio_links(senatepage)
    senate_bio_df = pd.DataFrame(senate_bio_infos)
    senate_bio_links = [(d['state_url']) for d in senate_bio_infos]

    with Pool() as pool:
        sen_data = pool.map(func=find_senate_info, iterable=senate_bio_links)
    sen_data = filter(lambda x: x != None, sen_data)

    sen_df = pd.DataFrame(sen_data)
    # print(sen_df)

    sen_df = pd.merge(sen_df, senate_bio_df, how='left', on=['state_url'])

    senate_wiki_link = 'https://en.wikipedia.org/wiki/Senate_of_Virginia#:~:text=The%20Senate%20of%20Virginia%20is,the%20lieutenant%20governor%20of%20Virginia.'
    sen_bios_wiki = get_senate_wiki_bios(senate_wiki_link)

    with Pool() as pool:
        sen_wiki_data = pool.map(
            func=find_wiki_sen_data, iterable=sen_bios_wiki)
    sen_wiki_df = pd.DataFrame(sen_wiki_data)
    mergedSensData = pd.merge(sen_df, sen_wiki_df, how='left', on=[
                              "name_first", "name_last"])
    mergedSensData['most_recent_term_id'] = mergedSensData['most_recent_term_id'].replace({
                                                                                          np.nan: None})
    mergedSensData['years_active'] = mergedSensData['years_active'].replace({
                                                                            np.nan: None})
    mergedSensData['occupation'] = mergedSensData['occupation'].replace({
                                                                        np.nan: None})
    mergedSensData['birthday'] = mergedSensData['birthday'].replace({
                                                                    np.nan: None})
    mergedSensData['education'] = mergedSensData['education'].replace({
                                                                      np.nan: None})
    sample_row = scraper_utils.initialize_row()
    # print(sample_row)
    #

    big_df = (mergedSensData.append(mergedRepsData, sort=True))
    big_df['state'] = sample_row.state
    big_df['state_id'] = sample_row.state_id
    #
    #
    big_df['country'] = sample_row.country
    # # #
    big_df['country_id'] = sample_row.country_id
    print(big_df)
    big_df['source_url'] = big_df['state_url']
    big_df['source_id'] = big_df['state_member_id']

    big_list_of_dicts = big_df.to_dict('records')
    # print(big_list_of_dicts)

    print('Writing data to database...')

    scraper_utils.insert_legislator_data_into_db(big_list_of_dicts)

    print('Complete!')
