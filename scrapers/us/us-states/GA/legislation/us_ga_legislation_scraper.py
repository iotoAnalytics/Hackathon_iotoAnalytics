"""
Due to changes in the website, this scraper currently doesn't fully work
"""

from datetime import datetime

from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup

from multiprocessing import Pool
from scraper_utils import USStateLegislationScraperUtils
import datetime

import PyPDF2
import requests
import io
import re

from nameparser import HumanName

state_abbreviation = 'GA'
database_table_name = 'us_ga_legislation'
legislator_table_name = 'us_ga_legislators'

scraper_utils = USStateLegislationScraperUtils(
    state_abbreviation, database_table_name, legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay('https://lis.virginia.gov/')



def get_bill_info(bill_url):
    state_url = bill_url
    uClient = uReq(bill_url)
    page_html = uClient.read()
    uClient.close()
    # # html parsing
    page_soup = soup(page_html, "html.parser")
    namediv = page_soup.find("div", {"class": "ggah1"})
    name_sesh_text = str(namediv.text)

    split_list = name_sesh_text.split(" - ")
    session = split_list[0]

    url_split = state_url.split("/")
    name_num = (url_split[8])

    name_title = split_list[1].split("\xa0")
    chamber_type = name_title[0]
    chamber_origin = ''
    if 'H' in chamber_type:
        chamber_origin = 'House'
    if 'S' in chamber_type:
        chamber_origin = 'Senate'
    bill_type = ''
    if 'R' in chamber_type:
        bill_type = 'Resolution'
    if 'B' in chamber_type:
        bill_type = 'Bill'
    s = name_title[1]
    match = re.compile("[^\W\d]").search(s)
    num_title = ([s[:match.start()], s[match.start():]])
    bill_name = chamber_type + name_num

    bill_title = num_title[1]

    # get sponsors
    sponsors = []
    sponsors_id = []
    sponsor_urls = []
    psu = ""
    sponlist = page_soup.findAll("span", {"style": "float:left; width:33%;"})
    for s in sponlist:
        spon = s.a.text
        sponurl = str(s.a["href"])
        sponurl = sponurl.strip()
        spon_split = re.split('(\d+)', spon)
        sponsor = spon_split[0]
        sponsor = HumanName(sponsor)
        sponsors.append(sponsor.last)
        sponsor_urls.append(sponurl)
        try:
            search_for = dict(source_url=sponurl, name_last=sponsor.last)

            sponsor_id = scraper_utils.get_legislator_id(**search_for)
            sponsors_id.append(sponsor_id)
        except:
            pass

    try:
        principal_sponsor = sponsors[0]

        search_for = dict(source_url=sponsor_urls[0], name_last=principal_sponsor)

        principal_sponsor_id = scraper_utils.get_legislator_id(**search_for)

    except:
        principal_sponsor_id = None
        principal_sponsor = ""

    # get committees
    chamber = ""
    committees = []
    comspan = page_soup.findAll("span", {"style": "float:left; width:49%;"})
    for c in comspan:
        try:

            if 'HC' in (c.text):
                chamber = 'House'
                committee = c.text.replace("HC: ", "")
            elif 'SC' in (c.text):
                chamber = 'Senate'
                committee = c.text.replace("SC: ", "")
            c = {'chamber': chamber, 'committee': committee}
            committees.append(c)
        except:
            pass

    # get bill summary
    bill_summary = ""
    all_items = page_soup.find("div", {"id": "content"})

    all_left_tags = all_items.findAll()
    for alt in all_left_tags:
        try:
            if (alt.b.i.text) == "First Reader Summary":
                ind = (all_left_tags.index(alt)) + 3
                bill_summary = (all_left_tags[ind]).text


        except:
            # print("failed")
            pass

    # get actions
    date_introduced = None
    actions = []
    for alt in all_left_tags:
        try:
            if alt.b.i.text == "Status History":

                ind = (all_left_tags.index(alt)) + 3
                events = ((all_left_tags[ind])).findAll("div")
                for event in events:

                    action_by = ""
                    event_info = event.text.split(" - ")

                    date = event_info[0]
                    d = datetime.datetime.strptime(date, "%b/%d/%Y").strftime("%Y-%m-%d")
                    date_introduced = d
                    # print(d)
                    description = event_info[1]

                    if "House" in description:
                        action_by = "House"
                        description = description.replace("House ", "")
                    if "Senate" in description:
                        action_by = "Senate"
                        description = description.replace("Senate ", "")

                    action = {"date": d, "action_by": action_by, 'description': description}
                    actions.append(action)

        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"

            message = template.format(type(ex).__name__, ex.args)

            # print(message)
    current_status = ""
    try:
        first_action = actions[0]
        current_status = first_action["description"]


    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)

        # print(message)

    # get bill_text
    screenonly = page_soup.find("div", {"class": "ScreenOnly"})
    pdf_link = (screenonly.a["href"])
    r = requests.get(pdf_link)
    f = io.BytesIO(r.content)
    reader = PyPDF2.PdfFileReader(f)

    contents = reader.getPage(0).extractText()
    bill_text = contents

    # get votes (if they have)
    v_info = []
    chamber = chamber_origin
    try:
        # get each section with a vote
        vote_sections = page_soup.findAll("span", {"style": "float:left; text-align:left; width:35%;"})
        for vs in vote_sections:
            info = vs.text.split(" - ")
            date = info[0]
            d = datetime.datetime.strptime(date, "%b/%d/%Y").strftime("%Y-%m-%d")
            date = d
            # print(date)
            votes_link = 'http://www.legis.ga.gov/' + vs.a["href"]
            # print(votes_link)
            description = vs.a.text

            if "House" in description:
                chamber = "House"
            elif "Senate" in description:
                chamber = "Senate"

            uClient = uReq(votes_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            v_soup = soup(page_html, "html.parser")

            y_votes = v_soup.find("span", {"class": "voteheader voteY"})
            yea = y_votes.text.replace("Yea (Y): ", "")

            n_votes = v_soup.find("span", {"class": "voteheader voteN voteHeaderSpacing"})
            nay = n_votes.text.replace("Nay (N): ", "")

            nv_votes = v_soup.find("span", {"class": "voteheader voteNV voteHeaderSpacing"})
            nv = nv_votes.text.replace("Not Voting (-): ", "")

            e_votes = v_soup.find("span", {"class": "voteheader voteE voteHeaderSpacing"})
            absent = e_votes.text.replace("Excused (E): ", "")

            total = int(yea) + int(nay) + int(nv) + int(absent)

            if int(yea) > int(nay):
                passed = 1
            else:
                passed = 0
            votes = []
            votelist = v_soup.findAll("li", {"class": "VoteList"})
            for vl in votelist:
                people = vl.findAll("span")
                voted = ""
                for p in people:
                    if p.text == "N":
                        voted = "nay"
                    if p.text == "Y":
                        voted = "yea"
                    if p.text == "-":
                        voted = "nv"
                    if p.text == "E":
                        voted = "excused"
                    pnext = (p.nextSibling)
                    leg_url = pnext.nextSibling["href"]
                    goverlytics_id = ""
                    legislator = pnext.nextSibling.text.split(",")[0]

                    legname = legislator.HumanName()
                    try:
                        search_for = dict(source_url=leg_url, name_last=legname.last)

                        goverlytics_id = scraper_utils.get_legislator_id(**search_for)
                    except:
                        pass

                    person = {'goverlytics_id': goverlytics_id, 'legislator': legislator, 'voted': voted}
                    votes.append(person)
            vote_info = {'date': date, 'description': description, 'yea': yea, 'nay': nay, 'nv': nv, 'absent': absent,
                         'total': total, 'passed': passed, 'chamber': chamber, 'votes': votes}
            v_info.append(vote_info)



    except Exception as ex:

        template = "An exception of type {0} occurred. Arguments:\n{1!r}"

        message = template.format(type(ex).__name__, ex.args)

        # print(message)

    # get goverlytics_id, url
    goverlytics_id = "GA_2019-2020_" + bill_name
    url = "/us/GA/legislation/" + goverlytics_id

    # make sure princ spon id is int
    try:
        principal_sponsor_id = int(principal_sponsor_id)
    except:
        pass

    bill_d = {'state_url': state_url, 'bill_type': bill_type, 'chamber_origin': chamber_origin,
                 'session': session, 'bill_name': bill_name, 'bill_title': bill_title, 'state': 'GA', 'state_id': 13,
                 'sponsors': sponsors, 'sponsors_id': sponsors_id, 'principal_sponsor': principal_sponsor,
                 'principal_sponsor_id': principal_sponsor_id, 'cosponsors': [], 'cosponsors_id': [],
                 'committees': committees, 'bill_state_id': "", 'bill_summary': bill_summary, 'actions': actions,
                 'date_introduced': date_introduced, 'bill_text': bill_text, 'bill_description': "", 'votes': v_info,
                 'site_topic': "", 'topic': "", 'current_status': current_status, 'goverlytics_id': goverlytics_id,
                 'url': url}

    print(bill_d)

    return bill_d


if __name__ == '__main__':
    # urls need to be updated for current session
    urls = []
    for i in range(1, 58):
        urls.append("https://www.legis.ga.gov/search?s=1029&p=" + str(i))

    print(urls)


    with Pool() as pool:
        # #
        bill_data = pool.map(func=get_bill_info, iterable=urls)
