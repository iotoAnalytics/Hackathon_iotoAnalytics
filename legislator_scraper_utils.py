import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
import pandas as pd
from database import Database
from dataclasses import dataclass, field
from typing import List
import copy
import atexit
from utils import DotDict

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislator
date collectors.
"""


# TODO Finish writing insert query for US federal scraper
# TODO Finish up canadian scrapers
# TODO Test state scraper. if it works, replace instance of ScraperUtils 
#   in existing scrapers with new class name. Be sure to remove country param.
# TODO can also remove country from config files and scraper scripts

@dataclass
class LegislatorRow:
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
    most_recent_term_id: str = ''
    name_full: str = ''
    name_last: str = ''
    name_first: str = ''
    name_middle: str = ''
    name_suffix: str = ''
    country_id: int = None
    country: str = ''
    party_id: int = None
    party: str = ''
    role: str = ''
    years_active: List[int] = field(default_factory=list)
    committees: List[dict] = field(default_factory=list)
    phone_number: List[dict] = field(default_factory=list)
    addresses: List[dict] = field(default_factory=list)
    email: str = ''
    birthday: datetime = None
    seniority: int = 0
    occupation: List[str] = field(default_factory=list)
    education: List[dict] = field(default_factory=list)
    military_experience: str = ''
    source_url: str = ''
    source_id: str = ''

@dataclass
class USLegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    state: str = ''
    state_id: int = None
    district: str = ''
    areas_served: List[str] = field(default_factory=list)


@dataclass
class CadLegislatorRow(LegislatorRow):
    """
    Data structure for housing data about each piece of legislator.
    """
    province_territory_id: int = None
    province_territory: str = ''
    riding: str = ''

class LegislatorScraperUtils():

    def __init__(self, country, database_table_name, row_type):
        
        # Database.initialise()
        self.db = Database()
        atexit.register(self.db.close_all_connections)

        # with CursorFromConnectionFromPool() as self.db.cur:
        try:
            query = 'SELECT * FROM countries'
            self.db.cur.execute(query)
            countries_results = self.db.cur.fetchall()

            query = f'SELECT * FROM {country}_parties'
            self.db.cur.execute(query)
            parties_results = self.db.cur.fetchall()

            query = f'SELECT * FROM {country}_divisions'
            self.db.cur.execute(query)
            division_results = self.db.cur.fetchall()
        except Exception as e:
            sys.exit(f'An exception occurred retrieving tables from database:\n{e}')

        self.countries = pd.DataFrame(countries_results)
        self.parties = pd.DataFrame(parties_results)
        self.divisions = pd.DataFrame(division_results)

        self.country = self.countries.loc[self.countries['abbreviation'] == country]['country'].values[0]
        self.country_id = int(self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])
        self.database_table_name = database_table_name
        self.row_type = row_type

    def _json_serial(self, obj):
        """
        Serializes date/datetime object. This is used to convert date and datetime objects to
        a format that can be digested by the database.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    def initialize_row(self):
        row = copy.copy(self.row_type)
        row.country_id = self.country_id
        row.country = self.country
        return row

    def get_attribute_id(self, table_name, column_to_search, value):
        accepted_tables = ['country', 'party', 'division']
        if table_name not in accepted_tables:
            raise Exception(f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'party':
            df = self.parties
        if table_name == 'division':
            df = self.divisions

        try:
            return int(df.loc[df[column_to_search] == value]['id'].values[0])
        except Exception as e:
            raise Exception(f'Error retrieving ID from table {table_name}: {e}')

    def get_party_id(self, party_name):
        """
        Used for getting the party ID number.
        """
        return self.get_attribute_id('party', 'party', party_name)
        

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
        return self.get_attribute_id('division', 'abbreviation', state_abbreviation)

    
    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

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

            self.db.cur.execute(create_table_query)
            self.db.conn.commit()
        except Exception as e:
            print(f'An exception occurred executing a query:\n{e}')

        insert_legislator_query = sql.SQL("""
                WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                INSERT INTO {table}
                VALUES (
                    (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    name_full = excluded.name_full,
                    name_last = excluded.name_last,
                    name_first = excluded.name_first,
                    name_middle = excluded.name_middle,
                    name_suffix = excluded.name_suffix,
                    country_id = excluded.country_id,
                    country = excluded.country,
                    district = excluded.district,
                    role = excluded.role,
                    committees = excluded.committees,
                    areas_served = excluded.areas_served,
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
                item = DotDict(item)

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
                json.dumps(item.committees, default=self._json_serial),
                item.areas_served,
                json.dumps(item.phone_number, default=self._json_serial),
                json.dumps(item.addresses, default=self._json_serial),
                item.email,
                item.birthday,
                item.seniority,
                item.occupation,
                json.dumps(item.education, default=self._json_serial),
                item.military_experience
            )

            self.db.cur.execute(insert_legislator_query, tup)
            

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


class CadFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name, country):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super.__init__(country, database_table_name, 'cad_parties', 'cad_province_territory_info')

    def initialize_row(self):
        '''
        Create a row and fill with empty values. This gets sent back to the scrape() function
        which then gets filled in with values collected from the website.
        '''

        row = CadFedLegislatorRow()

        try:
            row.country = self.country
            row.country_id = int(self.countries.loc[self.countries['country'] == self.country]['id'].values[0])
        except IndexError:
            sys.exit('An error occurred inserting state_id and/or country_id. Did you update the config file?')
        except Exception as e:
            sys.exit(f'An error occurred involving the state_id and/or country_id: {e}')

        return row


    def insert_legislator_data_into_db(self, data):
        """
        Takes care of inserting legislator data into database.
        """

        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of CadFedLegislatorRow')

        with CursorFromConnectionFromPool() as self.db.cur:
            try:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id bigint PRIMARY KEY,
                        state_member_id text,
                        most_recent_term_id text,
                        date_collected timestamp,
                        state_url TEXT UNIQUE,
                        url text,
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

                self.db.cur.execute(create_table_query)
                self.db.cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

            insert_legislator_query = sql.SQL("""
                WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                INSERT INTO {table}
                VALUES (
                    (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (state_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    name_full = excluded.name_full,
                    name_last = excluded.name_last,
                    name_first = excluded.name_first,
                    name_middle = excluded.name_middle,
                    name_suffix = excluded.name_suffix,
                    country_id = excluded.country_id,
                    country = excluded.country,
                    state_id = excluded.state_id,
                    state = excluded.state,
                    party_id = excluded.party_id,
                    party = excluded.party,
                    district = excluded.district,
                    role = excluded.role,
                    committees = excluded.committees,
                    areas_served = excluded.areas_served,
                    phone_number = excluded.phone_number,
                    addresses = excluded.addresses,
                    email = excluded.email,
                    birthday = excluded.birthday,
                    military_experience = excluded.military_experience,
                    occupation = excluded.occupation,
                    education = excluded.education,
                    state_member_id = excluded.source_id,
                    most_recent_term_id = excluded.most_recent_term_id,
                    years_active = excluded.years_active,
                    seniority = excluded.seniority;
                """).format(table=sql.Identifier(self.database_table_name), state=sql.SQL(self.state_abbreviation))

            date_collected = datetime.now()

            for row in data:
                if isinstance(row, USStateLegislatorRow):
                    try:

                        tup = (row.source_id, row.most_recent_term_id, date_collected, row.source_url,
                               row.name_full, row.name_last, row.name_first, row.name_middle, row.name_suffix,
                               row.country_id, row.country, row.state_id, row.state, row.party_id, row.party,
                               row.role, row.district, row.years_active,
                               json.dumps(row.committees, default=self._json_serial),
                               row.areas_served,
                               json.dumps(row.phone_number, default=self._json_serial),
                               json.dumps(row.addresses, default=self._json_serial),
                               row.email, row.birthday, row.seniority,
                               row.occupation,
                               json.dumps(row.education, default=self._json_serial),
                               row.military_experience)

                        self.db.cur.execute(insert_legislator_query, tup)


                    except Exception as e:
                        print(f'An exception occurred inserting {row.source_url}: {e}')

                elif isinstance(row, dict):
                    try:

                        tup = (row['state_member_id'], row['most_recent_term_id'], date_collected, row['state_url'],
                               row['name_full'], row['name_last'], row['name_first'], row['name_middle'],
                               row['name_suffix'],
                               row['country_id'], row['country'], row['state_id'], row['state'], row['party_id'],
                               row['party'],
                               row['role'], row['district'], row['years_active'],
                               json.dumps(row['committees'], default=self._json_serial),
                               row['areas_served'],
                               json.dumps(row['phone_number'], default=self._json_serial),
                               json.dumps(row['addresses'], default=self._json_serial),
                               row['email'], row['birthday'], row['seniority'],
                               row['occupation'],
                               json.dumps(row['education'], default=self._json_serial),
                               row['military_experience'])

                        self.db.cur.execute(insert_legislator_query, tup)
                    except Exception as e:
                        print(f'An exception occurred inserting {row["state_url"]}: {e}')
