import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
# import datetime
from datetime import date, datetime
import json
import sys
import pandas as pd
from database import Database, CursorFromConnectionFromPool
from dataclasses import dataclass, field
from typing import List
from rows import *
import copy
import atexit
import utils
from urllib.request import urlopen as uReq
import re
import unidecode
from bs4 import BeautifulSoup as soup
from nameparser import HumanName

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislator
date collectors.
"""


class LegislatorScraperUtils():

    def __init__(self, country, database_table_name, row_type):

        Database.initialise()
        atexit.register(Database.close_all_connections)

        with CursorFromConnectionFromPool() as cur:
            try:
                query = 'SELECT * FROM countries'
                cur.execute(query)
                countries_results = cur.fetchall()

                query = f'SELECT * FROM {country}_parties'
                cur.execute(query)
                parties_results = cur.fetchall()

                query = f'SELECT * FROM {country}_divisions'
                cur.execute(query)
                division_results = cur.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving tables from database:\n{e}')

        self.countries = pd.DataFrame(countries_results)
        self.parties = pd.DataFrame(parties_results)
        self.divisions = pd.DataFrame(division_results)

        self.country = self.countries.loc[self.countries['abbreviation'] == country]['country'].values[0]
        self.country_id = int(self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])
        self.database_table_name = database_table_name
        self.row_type = row_type

    def initialize_row(self):
        row = copy.deepcopy(self.row_type)
        row.country_id = self.country_id
        row.country = self.country
        return row

    def get_attribute(self, table_name, column_to_search, value, attribute='id'):
        accepted_tables = ['country', 'party', 'division']
        if table_name not in accepted_tables:
            raise Exception(f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'party':
            df = self.parties
        if table_name == 'division':
            df = self.divisions

        val = df.loc[df[column_to_search] == value][attribute].values[0]
        try:
            return int(val)
        except Exception:
            return val

    def get_party_id(self, party_name):
        """
        Used for getting the party ID number.
        """
        return self.get_attribute('party', 'party', party_name)

    def scrape_wiki_bio(self, wiki_link):
        """
        Used for getting missing legislator fields from their wikipedia bios.
        """
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            # #
            # # #grabs each product
            reps = page_soup.find("div", {"class": "mw-parser-output"})
            repBirth = reps.find("span", {"class": "bday"}).text

            b = datetime.strptime(repBirth, "%Y-%m-%d").date()

            birthday = b
            # print(b)

        except:
            # couldn't find birthday in side box
            birthday = None
        # get birthday another way
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            reps = page_soup.find("div", {"class": "mw-parser-output"})

            left_column_tags = reps.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if lefttag.text == "Born":
                    index = left_column_tags.index(lefttag) + 1
                    born = left_column_tags[index].text

                    if born != "Born":
                        b = datetime.strptime(born, "%Y-%m-%d").date()

                        birthday = b
                        print(b)

        except Exception as ex:

            pass

        # get years_active, based off of "assumed office"
        years_active = []
        year_started = ""
        try:
            uClient = uReq(wiki_link)
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
        lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD", "DDS", "MS", "BS", "MBA", "MS", "MD"]

        try:
            uClient = uReq(wiki_link)
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
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            # # #grabs each product
            head = page_soup.find("h1", {"id": "firstHeading"})
            name = head.text
            name = name.split("(")[0].strip()
            # name = name.replace(" (Canadian politician)", "")
            # name = name.replace(" (Quebec politician)", "")

        except:
            name = ""
        name = unidecode.unidecode(name)

        hN = HumanName(name)

        # get occupation
        occupation = []

        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

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

        most_recent_term_id = ""
        try:
            most_recent_term_id = (years_active[len(years_active) - 1])

        except:
            pass

        info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
                'education': education, 'occupation': occupation, 'years_active': years_active,
                'most_recent_term_id': str(most_recent_term_id)}

        """
            returns dictionary with the following fields, if available
            choose the ones that you weren't able to find from the gov website
            merge the resulting data with the data you scraped from the gov website
      
        """
        return info


class USFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name='us_fed_legislators'):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super().__init__('us', database_table_name, USLegislatorRow())

    def get_state_id(self, state_abbreviation):
        return self.get_attribute('division', 'abbreviation', state_abbreviation)

    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                        
                        
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            state_id int,
                            state char(2),
                            party_id int,
                            party text,
                            role text,
                            district text,
                            years_active int[],
                            committees jsonb,
                            areas_served text[],
                            phone_number jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        district = excluded.district,
                        role = excluded.role,
                        committees = excluded.committees,
                        areas_served = excluded.areas_served,
                        phone_number = excluded.phone_number,
                        addresses = excluded.addresses,
                        state = excluded.state,
                        state_id = excluded.state_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        seniority = excluded.seniority;
                    """).format(table=sql.Identifier(self.database_table_name))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                tup = (
                    item.source_id,
                    item.most_recent_term_id,
                    date_collected,
                    item.source_url,
                    item.name_full,
                    item.name_last,
                    item.name_first,
                    item.name_middle,
                    item.name_suffix,
                    item.country_id,
                    item.country,
                    item.state_id,
                    item.state,
                    item.party_id,
                    item.party,
                    item.role,
                    item.district,
                    item.years_active,
                    json.dumps(item.committees, default=utils.json_serial),
                    item.areas_served,
                    json.dumps(item.phone_number, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience
                )

                cur.execute(insert_legislator_query, tup)


class USStateLegislatorScraperUtils(USFedLegislatorScraperUtils):
    def __init__(self, state_abbreviation, database_table_name='us_state_legislators'):
        super().__init__(database_table_name)
        self.state = state_abbreviation
        self.state_id = self.get_state_id(state_abbreviation)

    def initialize_row(self):
        row = super().initialize_row()
        row.state = self.state
        row.state_id = self.state_id
        return row


class CAFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name='ca_fed_legislators', row_type=CAFedLegislatorRow()):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super().__init__('ca', database_table_name, row_type)

    def get_prov_terr_id(self, prov_terr_abbrev):
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev)

    def get_region(self, prov_terr_abbrev):
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev, 'region')

    def get_prov_terr_abbrev(self, prov_terr):
        return self.get_attribute('division', 'division', prov_terr, 'abbreviation')

    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                        
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            province_territory_id int,
                            province_territory char(2),
                            party_id int,
                            party text,
                            role text,
                            riding text,
                            years_active int[],
                            committees jsonb,
                            phone_number jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text,
                            region text,
                            offices_roles_as_mp text[],
                            parl_assoc_interparl_groups jsonb
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        riding = excluded.riding,
                        province_territory = excluded.province_territory,
                        province_territory_id = excluded.province_territory_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        role = excluded.role,
                        committees = excluded.committees,
                        phone_number = excluded.phone_number,
                        addresses = excluded.addresses,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        offices_roles_as_mp = excluded.offices_roles_as_mp,
                        parl_assoc_interparl_groups = excluded.parl_assoc_interparl_groups,
                        region = excluded.region,
                        seniority = excluded.seniority;
                    """).format(table=sql.Identifier(self.database_table_name))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                tup = (
                    item.source_id,
                    item.most_recent_term_id,
                    date_collected,
                    item.source_url,
                    item.name_full,
                    item.name_last,
                    item.name_first,
                    item.name_middle,
                    item.name_suffix,
                    item.country_id,
                    item.country,
                    item.province_territory_id,
                    item.province_territory,
                    item.party_id,
                    item.party,
                    item.role,
                    item.riding,
                    item.years_active,
                    json.dumps(item.committees, default=utils.json_serial),
                    json.dumps(item.phone_number, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience,
                    item.region,
                    item.offices_roles_as_mp,
                    json.dumps(item.parl_assoc_interparl_groups, default=utils.json_serial)
                )

                cur.execute(insert_legislator_query, tup)


class CAProvTerrLegislatorScraperUtils(CAFedLegislatorScraperUtils):
    def __init__(self, prov_terr_abbreviation, database_table_name='ca_provterr_legislators'):
        super().__init__(database_table_name, CALegislatorRow())
        self.province_territory = prov_terr_abbreviation
        self.province_territory_id = self.get_prov_terr_id(prov_terr_abbreviation)

    def initialize_row(self):
        row = super().initialize_row()
        row.province_territory = self.province_territory
        row.province_territory_id = self.province_territory_id
        return row

    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                        
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            province_territory_id int,
                            province_territory char(2),
                            party_id int,
                            party text,
                            role text,
                            riding text,
                            years_active int[],
                            committees jsonb,
                            phone_number jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text,
                            region text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
                    
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        riding = excluded.riding,
                        province_territory = excluded.province_territory,
                        province_territory_id = excluded.province_territory_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        role = excluded.role,
                        committees = excluded.committees,
                        phone_number = excluded.phone_number,
                        addresses = excluded.addresses,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        seniority = excluded.seniority;
                    """).format(table=sql.Identifier(self.database_table_name))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                tup = (
                    item.source_id,
                    item.most_recent_term_id,
                    date_collected,
                    item.source_url,
                    item.name_full,
                    item.name_last,
                    item.name_first,
                    item.name_middle,
                    item.name_suffix,
                    item.country_id,
                    item.country,
                    item.province_territory_id,
                    item.province_territory,
                    item.party_id,
                    item.party,
                    item.role,
                    item.riding,
                    item.years_active,
                    json.dumps(item.committees, default=utils.json_serial),
                    json.dumps(item.phone_number, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience,
                    item.region
                )

                cur.execute(insert_legislator_query, tup)
